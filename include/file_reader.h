#ifndef FILE_READER_H
#define FILE_READER_H

#include <string>
#include <vector>
#include "tpch_structs.h"

// Region
std::vector<Region> readRegions(const std::string& filepath);

// Nation
std::vector<Nation> readNations(const std::string& filepath);

// Customer
std::vector<Customer> readCustomers(const std::string& filepath);

// Supplier
std::vector<Supplier> readSuppliers(const std::string& filepath);

// LineItem
std::vector<LineItem> readLineItems(const std::string& filepath);

// Orders
std::vector<Order> readOrders(const std::string& filepath);

#endif
