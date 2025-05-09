#include "../include/file_reader.h"
#include <bits/stdc++.h>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>

//
// Reads data from region.tbl
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
// Reads data from nation.tbl
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
// Reads data from customer.tbl
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
// Reads data from orders.tbl
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

            // Skip 3 columns: order status, total price, order priority
            for (int i = 0; i < 3; ++i) std::getline(ss, token, '|');

            std::getline(ss, o.o_orderdate, '|'); // Get order date
            orders.push_back(o);
        } catch (const std::exception& e) {
            std::cerr << "Error in order line: " << line << "\nReason: " << e.what() << "\n";
        }
    }

    return orders;
}

//
// Reads data from lineitem.tbl
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
            std::getline(ss, token, '|'); // skip partkey
            std::getline(ss, token, '|'); li.l_suppkey = std::stoi(token);
            std::getline(ss, token, '|'); // skip linenumber
            std::getline(ss, token, '|'); // skip quantity
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
// Reads data from supplier.tbl
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
            // Format: s_suppkey|s_name|address|nationkey|phone|acctbal|comment
            std::getline(ss, token, '|'); s.s_suppkey = std::stoi(token);
            std::getline(ss, token, '|'); // skip s_name
            std::getline(ss, token, '|'); // skip address
            std::getline(ss, token, '|'); s.s_nationkey = std::stoi(token);
            std::getline(ss, token, '|'); // skip phone
            std::getline(ss, token, '|'); s.s_acctbal = std::stod(token);

            suppliers.push_back(s);
        } catch (const std::exception& e) {
            std::cerr << "Error in supplier line: " << line << "\nReason: " << e.what() << "\n";
        }
    }

    return suppliers;
}