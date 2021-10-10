/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */


#include <boost/test/unit_test.hpp>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include "test/boost/sstable_test.hh"
#include <seastar/core/thread.hh>
#include "sstables/sstables.hh"
#include "test/lib/mutation_source_test.hh"
#include "test/lib/sstable_utils.hh"
#include "row_cache.hh"
#include "test/lib/simple_schema.hh"
#include "partition_slice_builder.hh"
#include "test/lib/flat_mutation_reader_assertions.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/random_schema.hh"

using namespace sstables;
using namespace std::chrono_literals;

static
mutation_source make_sstable_mutation_source(sstables::test_env& env, schema_ptr s, sstring dir, std::vector<mutation> mutations,
        sstable_writer_config cfg, sstables::sstable::version_types version, gc_clock::time_point query_time = gc_clock::now()) {
    return as_mutation_source(make_sstable(env, s, dir, std::move(mutations), cfg, version, query_time));
}

static void consume_all(flat_mutation_reader& rd) {
    while (auto mfopt = rd().get0()) {}
}

// It is assumed that src won't change.
static snapshot_source snapshot_source_from_snapshot(mutation_source src) {
    return snapshot_source([src = std::move(src)] {
        return src;
    });
}

static
void test_cache_population_with_range_tombstone_adjacent_to_population_range(populate_fn_ex populate) {
    simple_schema s;
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto cache_mt = make_lw_shared<memtable>(s.schema());

    auto pkey = s.make_pkey();

    // underlying should not be empty, otherwise cache will make the whole range continuous
    mutation m1(s.schema(), pkey);
    s.add_row(m1, s.make_ckey(0), "v1");
    s.add_row(m1, s.make_ckey(1), "v2");
    s.add_row(m1, s.make_ckey(2), "v3");
    s.delete_range(m1, s.make_ckey_range(2, 100));
    cache_mt->apply(m1);

    cache_tracker tracker;
    auto ms = populate(s.schema(), std::vector<mutation>({m1}), gc_clock::now());
    row_cache cache(s.schema(), snapshot_source_from_snapshot(std::move(ms)), tracker);

    auto pr = dht::partition_range::make_singular(pkey);

    auto populate_range = [&] (int start) {
        auto slice = partition_slice_builder(*s.schema())
                .with_range(query::clustering_range::make_singular(s.make_ckey(start)))
                .build();
        auto rd = cache.make_reader(s.schema(), semaphore.make_permit(), pr, slice);
        auto close_rd = deferred_close(rd);
        consume_all(rd);
    };

    populate_range(2);

    // The cache now has only row with ckey 2 populated and the rest is discontinuous.
    // Populating reader which stops populating at entry with ckey 2 should not forget
    // to emit range_tombstone which starts at before(2).

    assert_that(cache.make_reader(s.schema(), semaphore.make_permit()))
            .produces(m1)
            .produces_end_of_stream();
}

static future<> test_sstable_conforms_to_mutation_source(sstable_version_types version, int index_block_size) {
    return sstables::test_env::do_with_async([version, index_block_size] (sstables::test_env& env) {
        sstable_writer_config cfg = env.manager().configure_writer();
        cfg.promoted_index_block_size = index_block_size;

        std::vector<tmpdir> dirs;
        auto populate = [&env, &dirs, &cfg, version] (schema_ptr s, const std::vector<mutation>& partitions,
                                                      gc_clock::time_point query_time) -> mutation_source {
            dirs.emplace_back();
            return make_sstable_mutation_source(env, s, dirs.back().path().string(), partitions, cfg, version, query_time);
        };

        run_mutation_source_tests(populate);

        if (index_block_size == 1) {
            // The tests below are not sensitive to index bock size so run once.
            test_cache_population_with_range_tombstone_adjacent_to_population_range(populate);
        }
    });
}

static constexpr std::array<int, 3> block_sizes = { 1, 128, 64 * 1024 };

// Split for better parallelizm

SEASTAR_TEST_CASE(test_sstable_conforms_to_mutation_source_mc_tiny) {
    return test_sstable_conforms_to_mutation_source(writable_sstable_versions[0], block_sizes[0]);
}

SEASTAR_TEST_CASE(test_sstable_conforms_to_mutation_source_mc_medium) {
    return test_sstable_conforms_to_mutation_source(writable_sstable_versions[0], block_sizes[1]);
}

SEASTAR_TEST_CASE(test_sstable_conforms_to_mutation_source_mc_large) {
    return test_sstable_conforms_to_mutation_source(writable_sstable_versions[0], block_sizes[2]);
}

SEASTAR_TEST_CASE(test_sstable_conforms_to_mutation_source_md_tiny) {
    return test_sstable_conforms_to_mutation_source(writable_sstable_versions[1], block_sizes[0]);
}

SEASTAR_TEST_CASE(test_sstable_conforms_to_mutation_source_md_medium) {
    return test_sstable_conforms_to_mutation_source(writable_sstable_versions[1], block_sizes[1]);
}

SEASTAR_TEST_CASE(test_sstable_conforms_to_mutation_source_md_large) {
    return test_sstable_conforms_to_mutation_source(writable_sstable_versions[1], block_sizes[2]);
}

// This assert makes sure we don't miss writable vertions
static_assert(writable_sstable_versions.size() == 2);

// TODO: could probably be placed in random_utils/random_schema after some tuning
static std::vector<query::clustering_range> random_ranges(const std::vector<clustering_key> keys, const schema& s, std::mt19937& engine) {
    std::vector<clustering_key_prefix> ckp_sample;
    for (auto& k: keys) {
        auto exploded = k.explode(s);
        auto key_size = tests::random::get_int<size_t>(1, exploded.size(), engine);
        ckp_sample.push_back(clustering_key_prefix::from_exploded(s,
            std::vector<bytes>{exploded.begin(), exploded.begin() + key_size}));
    }

    auto subset_size = tests::random::get_int<size_t>(0, ckp_sample.size(), engine);
    ckp_sample = tests::random::random_subset<clustering_key_prefix>(std::move(ckp_sample), subset_size, engine);
    std::sort(ckp_sample.begin(), ckp_sample.end(), clustering_key_prefix::less_compare(s));

    std::vector<query::clustering_range> ranges;

    // (-inf, ...
    if (!ckp_sample.empty() && tests::random::get_bool(engine)) {
        ranges.emplace_back(
                std::nullopt,
                query::clustering_range::bound{ckp_sample.front(), tests::random::get_bool(engine)});
        std::swap(ckp_sample.front(), ckp_sample.back());
        ckp_sample.pop_back();
    }

    // ..., +inf)
    std::optional<query::clustering_range> last_range;
    if (!ckp_sample.empty() && tests::random::get_bool(engine)) {
        last_range.emplace(
                query::clustering_range::bound{ckp_sample.back(), tests::random::get_bool(engine)},
                std::nullopt);
        ckp_sample.pop_back();
    }

    for (unsigned i = 0; i + 1 < ckp_sample.size(); i += 2) {
        ranges.emplace_back(
                query::clustering_range::bound{ckp_sample[i], tests::random::get_bool(engine)},
                query::clustering_range::bound{ckp_sample[i+1], tests::random::get_bool(engine)});
    }

    if (last_range) {
        ranges.push_back(std::move(*last_range));
    }

    if (ranges.empty()) {
        // no keys sampled, return (-inf, +inf)
        ranges.push_back(query::clustering_range::make_open_ended_both_sides());
    }

    return ranges;
}

SEASTAR_THREAD_TEST_CASE(test_sstable_reversing_reader_random_schema) {
    // Create two sources: one by creating an sstable from a set of mutations,
    // and one by creating an sstable from the same set of mutations but reversed.
    // We query the first source in forward mode and the second in reversed mode.
    // The two queries should return the same result.

    auto random_spec = tests::make_random_schema_specification(get_name());
    auto random_schema = tests::random_schema{tests::random::get_int<uint32_t>(), *random_spec};
    testlog.debug("Random schema:\n{}", random_schema.cql());

    auto muts = tests::generate_random_mutations(random_schema).get();

    std::vector<mutation> reversed_muts;
    for (auto& m : muts) {
        reversed_muts.push_back(reverse(m));
    }

    // FIXME: workaround for #9352. The index pages for reversed source would sometimes be different
    // from the forward source, causing one source to hit the bug from #9352 but not the other.
    muts.erase(std::remove_if(muts.begin(), muts.end(), [] (auto& m) { return m.decorated_key().token() == dht::token::from_int64(0); }));

    sstables::test_env::do_with([
            s = random_schema.schema(), muts = std::move(muts), reversed_muts = std::move(reversed_muts),
            version = writable_sstable_versions[1]] (sstables::test_env& env) {

        std::vector<tmpdir> dirs;
        sstable_writer_config cfg = env.manager().configure_writer();

        for (auto index_block_size: block_sizes) {
            cfg.promoted_index_block_size = index_block_size;

            dirs.emplace_back();
            auto source = make_sstable_mutation_source(env, s, dirs.back().path().string(), muts, cfg, version);

            dirs.emplace_back();
            auto rev_source = make_sstable_mutation_source(env, s->make_reversed(), dirs.back().path().string(), reversed_muts, cfg, version);

            tests::reader_concurrency_semaphore_wrapper semaphore;

            std::mt19937 engine{tests::random::get_int<uint32_t>()};

            std::vector<clustering_key> keys;
            for (const auto& m: muts) {
                for (auto& r: m.partition().clustered_rows()) {
                    keys.push_back(r.key());
                }
            }

            auto slice = partition_slice_builder(*s)
                .with_ranges(random_ranges(keys, *s, engine))
                .build();

            testlog.trace("Slice: {}", slice);

            for (const auto& m: muts) {
                auto prange = dht::partition_range::make_singular(m.decorated_key());

                auto r1 = source.make_reader(s, semaphore.make_permit(), prange,
                        slice, default_priority_class(), nullptr,
                        streamed_mutation::forwarding::no, mutation_reader::forwarding::no);

                auto rev_slice = native_reverse_slice_to_legacy_reverse_slice(*s, slice);
                rev_slice.options.set(query::partition_slice::option::reversed);
                auto r2 = rev_source.make_reader(s, semaphore.make_permit(), prange,
                        rev_slice, default_priority_class(), nullptr,
                        streamed_mutation::forwarding::no, mutation_reader::forwarding::no);

                compare_readers(*s, std::move(r1), std::move(r2));
            }
        }
    }).get();
}
