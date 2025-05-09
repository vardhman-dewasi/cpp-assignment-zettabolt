#ifndef TPCH_STRUCTS_H
#define TPCH_STRUCTS_H

#include <string>

struct Region {
    int r_regionkey;
    std::string r_name;
};

struct Nation {
    int n_nationkey;
    std::string n_name;
    int n_regionkey;
};

struct Customer {
    int c_custkey;
    int c_nationkey;
};

struct Order {
    int o_orderkey;
    int o_custkey;
    std::string o_orderdate;
};

struct LineItem {
    int l_orderkey;
    int l_suppkey;
    double l_extendedprice;
    double l_discount;
};

struct Supplier {
    int s_suppkey;
    int s_nationkey;
    double s_acctbal;
};

#endif // TPCH_STRUCTS_H
