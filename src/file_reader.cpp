#include "../include/file_reader.h"
#include <bits/stdc++.h>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>

//
// Function to read data from region.tbl
// Each line format: r_regionkey|r_name|r_comment|
// Only r_regionkey and r_name are extracted
//
std::vector<Region> readRegions(const std::string& path) {
    std::ifstream file(path);
    std::vector<Region> regions;
    std::string line;

    while (std::getline(file, line)) {
        if (line.empty()) continue;

        std::stringstream ss(line);
        Region r;
        std::string token;

        try {
            std::getline(ss, token, '|'); r.r_regionkey = std::stoi(token);
            std::getline(ss, r.r_name, '|');
            regions.push_back(r);
        } catch (const std::exception& e) {
            std::cerr << "Error in region line: " << line << "\nReason: " << e.what() << "\n";
        }
    }

    return regions;
}

//
// Function to read data from nation.tbl
// Each line format: n_nationkey|n_name|n_regionkey|n_comment|
// Only n_nationkey, n_name, n_regionkey are used
//
std::vector<Nation> readNations(const std::string& path) {
    std::ifstream file(path);
    std::vector<Nation> nations;
    std::string line;

    while (std::getline(file, line)) {
        if (line.empty()) continue;

        std::stringstream ss(line);
        Nation n;
        std::string token;

        try {
            std::getline(ss, token, '|'); n.n_nationkey = std::stoi(token);
            std::getline(ss, n.n_name, '|');
            std::getline(ss, token, '|'); n.n_regionkey = std::stoi(token);
            nations.push_back(n);
        } catch (const std::exception& e) {
            std::cerr << "Error in nation line: " << line << "\nReason: " << e.what() << "\n";
        }
    }

    return nations;
}

//
// Function to read data from customer.tbl
// Each line format: c_custkey|c_name|c_address|c_nationkey|...|
// Only c_custkey and c_nationkey are extracted
//
std::vector<Customer> readCustomers(const std::string& path) {
    std::ifstream file(path);
    std::vector<Customer> customers;
    std::string line;

    while (std::getline(file, line)) {
        if (line.empty()) continue;

        std::stringstream ss(line);
        Customer c;
        std::string token;

        try {
            std::getline(ss, token, '|'); c.c_custkey = std::stoi(token);
            std::getline(ss, token, '|'); // skip c_name
            std::getline(ss, token, '|'); // skip c_address
            std::getline(ss, token, '|'); c.c_nationkey = std::stoi(token);
            customers.push_back(c);
        } catch (const std::exception& e) {
            std::cerr << "Error in customer line: " << line << "\nReason: " << e.what() << "\n";
        }
    }

    return customers;
}

//
// Function to read data from orders.tbl
// Each line format: o_orderkey|o_custkey|o_orderstatus|o_totalprice|o_orderdate|...
// Only o_orderkey, o_custkey, and o_orderdate are extracted
//
std::vector<Order> readOrders(const std::string& path) {
    std::ifstream file(path);
    std::vector<Order> orders;
    std::string line;

    while (std::getline(file, line)) {
        if (line.empty()) continue;

        std::stringstream ss(line);
        Order o;
        std::string token;

        try {
            std::getline(ss, token, '|'); o.o_orderkey = std::stoi(token);
            std::getline(ss, token, '|'); o.o_custkey = std::stoi(token);

            // Skip 2 columns: o_orderstatus, o_totalprice
            for (int i = 0; i < 2; ++i) std::getline(ss, token, '|');

            std::getline(ss, o.o_orderdate, '|'); // Extract order date
            orders.push_back(o);
        } catch (const std::exception& e) {
            std::cerr << "Error in order line: " << line << "\nReason: " << e.what() << "\n";
        }
    }

    return orders;
}

//
// Function to read data from lineitem.tbl
// Each line format: l_orderkey|l_partkey|l_suppkey|l_linenumber|l_quantity|l_extendedprice|l_discount|...
// Only l_orderkey, l_suppkey, l_extendedprice, l_discount are extracted
//
std::vector<LineItem> readLineItems(const std::string& path) {
    std::ifstream file(path);
    std::vector<LineItem> items;
    std::string line;

    while (std::getline(file, line)) {
        if (line.empty()) continue;

        std::stringstream ss(line);
        LineItem li;
        std::string token;

        try {
            std::getline(ss, token, '|'); li.l_orderkey = std::stoi(token);
            std::getline(ss, token, '|'); // skip l_partkey
            std::getline(ss, token, '|'); li.l_suppkey = std::stoi(token);
            std::getline(ss, token, '|'); // skip l_linenumber
            std::getline(ss, token, '|'); // skip l_quantity
            std::getline(ss, token, '|'); li.l_extendedprice = std::stod(token);
            std::getline(ss, token, '|'); li.l_discount = std::stod(token);

            items.push_back(li);
        } catch (const std::exception& e) {
            std::cerr << "Error in lineitem line: " << line << "\nReason: " << e.what() << "\n";
        }
    }

    return items;
}

//
// Function to read data from supplier.tbl
// Each line format: s_suppkey|s_name|s_address|s_nationkey|s_phone|s_acctbal|s_comment|
// Only s_suppkey, s_nationkey, and s_acctbal are extracted
//
std::vector<Supplier> readSuppliers(const std::string& path) {
    std::ifstream file(path);
    std::vector<Supplier> suppliers;
    std::string line;

    while (std::getline(file, line)) {
        if (line.empty()) continue;

        std::stringstream ss(line);
        Supplier s;
        std::string token;

        try {
            std::getline(ss, token, '|'); s.s_suppkey = std::stoi(token);
            std::getline(ss, token, '|'); // skip s_name
            std::getline(ss, token, '|'); // skip s_address
            std::getline(ss, token, '|'); s.s_nationkey = std::stoi(token);
            std::getline(ss, token, '|'); // skip s_phone
            std::getline(ss, token, '|'); s.s_acctbal = std::stod(token);

            suppliers.push_back(s);
        } catch (const std::exception& e) {
            std::cerr << "Error in supplier line: " << line << "\nReason: " << e.what() << "\n";
        }
    }

    return suppliers;
}
