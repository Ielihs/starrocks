// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/range.h"

#include "gtest/gtest.h"

namespace starrocks::vectorized {

inline std::string to_bitmap_string(const uint8_t* bitmap, size_t n) {
    std::string s;
    for (size_t i = 0; i < n; i++) {
        s.push_back(bitmap[i] ? '1' : '0');
    }
    return s;
}

TEST(SparseRangeTest, range_union) {
    SparseRange range;
    ASSERT_TRUE(range.empty());
    EXPECT_EQ(0, range.span_size());

    // Add an empty range: ignored
    range.add({10, 0});
    ASSERT_TRUE(range.empty());
    EXPECT_EQ(0u, range.span_size());

    // Add [10, 20)
    range.add({10, 20});
    ASSERT_FALSE(range.empty());
    EXPECT_EQ(10u, range.span_size());
    EXPECT_EQ("([10,20))", range.to_string());

    // Add [30, 40)
    range.add({30, 40});
    EXPECT_EQ(20u, range.span_size());
    EXPECT_EQ("([10,20), [30,40))", range.to_string());

    // Add [40, 50)
    range.add({40, 50});
    EXPECT_EQ("([10,20), [30,50))", range.to_string());

    // Add [8, 9)
    range.add({8, 9});
    EXPECT_EQ("([8,9), [10,20), [30,50))", range.to_string());

    // Add [25, 27)
    range.add({25, 27});
    EXPECT_EQ("([8,9), [10,20), [25,27), [30,50))", range.to_string());

    // Add [9, 21)
    range.add({9, 21});
    EXPECT_EQ("([8,21), [25,27), [30,50))", range.to_string());

    // Add [28, 60)
    range.add({28, 60});
    EXPECT_EQ("([8,21), [25,27), [28,60))", range.to_string());

    // Add [27, 28)
    range.add({27, 28});
    EXPECT_EQ("([8,21), [25,60))", range.to_string());

    // Add [6, 70)
    range.add({6, 70});
    EXPECT_EQ("([6,70))", range.to_string());
}

TEST(SparseRangeTest, range_intersection) {
    SparseRange r1({{1, 10}, {20, 40}, {50, 70}});
    SparseRange r2{{0, 100}};
    SparseRange r3{};
    SparseRange r4{{2, 30}};
    SparseRange r5{{1, 20}, {25, 26}, {30, 65}};

    auto r = r1.intersection(r1);
    EXPECT_EQ(r1, r);

    r = r1.intersection(r2);
    EXPECT_EQ(r1, r);

    r = r1.intersection(r3);
    EXPECT_EQ(SparseRange(), r);

    r = r1.intersection(r4);
    EXPECT_EQ(SparseRange({{2, 10}, {20, 30}}), r);

    r = r1.intersection(r5);
    EXPECT_EQ(SparseRange({{1, 10}, {25, 26}, {30, 40}, {50, 65}}), r);
}

TEST(SparseRangeIteratorTest, covered_ranges) {
    SparseRange r1({{0, 10}, {20, 40}, {50, 70}});
    SparseRangeIterator iter = r1.new_iterator();
    EXPECT_EQ(0, iter.covered_ranges(0));
    for (int i = 1; i <= 20; i++) {
        EXPECT_EQ(1u, iter.covered_ranges(i)) << "i=" << i;
    }
    for (int i = 21; i <= 50; i++) {
        EXPECT_EQ(2u, iter.covered_ranges(i)) << "i=" << i;
    }
    for (int i = 51; i <= 100; i++) {
        EXPECT_EQ(3u, iter.covered_ranges(i)) << "i=" << i;
    }

    (void)iter.next(5);
    // [5, 10), [20, 40), [50, 70)
    EXPECT_EQ(0, iter.covered_ranges(0));
    for (int i = 1; i <= 15; i++) {
        EXPECT_EQ(1u, iter.covered_ranges(i)) << "i=" << i;
    }
    for (int i = 16; i <= 45; i++) {
        EXPECT_EQ(2u, iter.covered_ranges(i)) << "i=" << i;
    }
    for (int i = 46; i <= 100; i++) {
        EXPECT_EQ(3u, iter.covered_ranges(i)) << "i=" << i;
    }

    (void)iter.next(10);
    // [20, 40), [50, 70)
    EXPECT_EQ(0, iter.covered_ranges(0));
    for (int i = 1; i <= 30; i++) {
        EXPECT_EQ(1u, iter.covered_ranges(i)) << "i=" << i;
    }
    for (int i = 31; i <= 55; i++) {
        EXPECT_EQ(2u, iter.covered_ranges(i)) << "i=" << i;
    }
}

TEST(SparseRangeIteratorTest, convert_to_bitmap) {
    std::vector<uint8_t> bitmap(100, 0);
    SparseRange r1({{1, 11}, {20, 22}, {24, 25}});
    SparseRangeIterator iter = r1.new_iterator();

    ASSERT_EQ(0u, iter.convert_to_bitmap(bitmap.data(), 0));

    ASSERT_EQ(5u, iter.convert_to_bitmap(bitmap.data(), 5));
    ASSERT_EQ("11111", to_bitmap_string(bitmap.data(), 5));

    ASSERT_EQ(10u, iter.convert_to_bitmap(bitmap.data(), 10));
    ASSERT_EQ("1111111111", to_bitmap_string(bitmap.data(), 10));

    ASSERT_EQ(15u, iter.convert_to_bitmap(bitmap.data(), 15));
    ASSERT_EQ("111111111100000", to_bitmap_string(bitmap.data(), 15));

    ASSERT_EQ(19u, iter.convert_to_bitmap(bitmap.data(), 19));
    ASSERT_EQ("1111111111000000000", to_bitmap_string(bitmap.data(), 19));

    ASSERT_EQ(20u, iter.convert_to_bitmap(bitmap.data(), 20));
    ASSERT_EQ("11111111110000000001", to_bitmap_string(bitmap.data(), 20));

    ASSERT_EQ(21u, iter.convert_to_bitmap(bitmap.data(), 21));
    ASSERT_EQ("111111111100000000011", to_bitmap_string(bitmap.data(), 21));

    ASSERT_EQ(22u, iter.convert_to_bitmap(bitmap.data(), 22));
    ASSERT_EQ("1111111111000000000110", to_bitmap_string(bitmap.data(), 22));

    ASSERT_EQ(23u, iter.convert_to_bitmap(bitmap.data(), 23));
    ASSERT_EQ("11111111110000000001100", to_bitmap_string(bitmap.data(), 23));

    ASSERT_EQ(24u, iter.convert_to_bitmap(bitmap.data(), 24));
    ASSERT_EQ("111111111100000000011001", to_bitmap_string(bitmap.data(), 24));

    ASSERT_EQ(24u, iter.convert_to_bitmap(bitmap.data(), 25));
    ASSERT_EQ("111111111100000000011001", to_bitmap_string(bitmap.data(), 24));

    ASSERT_EQ(24u, iter.convert_to_bitmap(bitmap.data(), 26));
    ASSERT_EQ("111111111100000000011001", to_bitmap_string(bitmap.data(), 24));
}

} // namespace starrocks::vectorized
