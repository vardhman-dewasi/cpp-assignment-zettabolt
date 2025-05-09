#include "../include/tpch_structs.h"
#include "file_reader.h"
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
        std::stringstream ss(line);
        Region r;
        // Format: r_regionkey|r_name|...
        std::getline(ss, line, '|'); r.r_regionkey = std::stoi(line);
        std::getline(ss, r.r_name, '|');
        regions.push_back(r);
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
        std::stringstream ss(line);
        Nation n;
        // Format: n_nationkey|n_name|n_regionkey|...
        std::getline(ss, line, '|'); n.n_nationkey = std::stoi(line);
        std::getline(ss, n.n_name, '|');
        std::getline(ss, line, '|'); n.n_regionkey = std::stoi(line);
        nations.push_back(n);
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
        std::stringstream ss(line);
        Customer c;
        // Format: c_custkey|...|c_nationkey|...
        std::getline(ss, line, '|'); c.c_custkey = std::stoi(line);
        std::getline(ss, line, '|'); // skip c_name
        std::getline(ss, line, '|'); c.c_nationkey = std::stoi(line);
        customers.push_back(c);
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
        std::stringstream ss(line);
        Order o;
        // Format: o_orderkey|o_custkey|...|o_orderdate|...
        std::getline(ss, line, '|'); o.o_orderkey = std::stoi(line);
        std::getline(ss, line, '|'); o.o_custkey = std::stoi(line);

        // Skip 3 columns: order status, total price, order priority
        for (int i = 0; i < 3; ++i) std::getline(ss, line, '|');

        std::getline(ss, o.o_orderdate, '|'); // Get order date
        orders.push_back(o);
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
        std::stringstream ss(line);
        LineItem li;
        // Format: l_orderkey|...|l_suppkey|...|l_extendedprice|l_discount|...
        std::getline(ss, line, '|'); li.l_orderkey = std::stoi(line);

        std::getline(ss, line, '|'); // skip partkey
        std::getline(ss, line, '|'); li.l_suppkey = std::stoi(line);

        std::getline(ss, line, '|'); // skip quantity
        std::getline(ss, line, '|'); li.l_extendedprice = std::stod(line);
        std::getline(ss, line, '|'); li.l_discount = std::stod(line);

        items.push_back(li);
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
        std::stringstream ss(line);
        Supplier s;
        // Format: s_suppkey|...|s_nationkey|...
        std::getline(ss, line, '|'); s.s_suppkey = std::stoi(line);
        std::getline(ss, line, '|'); // skip s_name
        std::getline(ss, line, '|'); s.s_nationkey = std::stoi(line);
        suppliers.push_back(s);
    }

    return suppliers;
}
