#include "../include/tpch_structs.h"
#include "../include/file_reader.h"
#include <bits/stdc++.h>
#include <mutex>
#include <thread>
#include <chrono>
#include <fstream>

std::mutex mtx; // For synchronizing shared data access

void processChunk(
    const std::vector<Order>& orders,
    const std::unordered_map<int, int>& custToNation,
    const std::unordered_map<int, std::string>& nationNames,
    const std::unordered_map<int, int>& nationToRegion,
    const std::unordered_map<int, std::string>& regionNames,
    const std::unordered_map<int, std::vector<LineItem>>& orderToLineItems,
    const std::unordered_map<int, int>& suppToNation,
    const std::string& regionFilter,
    const std::string& startDate,
    const std::string& endDate,
    std::unordered_map<std::string, double>& localRevenue,
    int start, int end
) {
    std::unordered_map<std::string, double> localMap;

    for (int i = start; i < end; ++i) {
        const Order& o = orders[i];

        if (o.o_orderdate >= startDate && o.o_orderdate < endDate) {
            int custKey = o.o_custkey;
            if (custToNation.count(custKey) == 0) continue;
            int nationKey = custToNation.at(custKey);

            if (nationToRegion.count(nationKey) == 0) continue;
            int regionKey = nationToRegion.at(nationKey);
            std::string regionName = regionNames.at(regionKey);
            if (regionName != regionFilter) continue;

            if (orderToLineItems.count(o.o_orderkey) == 0) continue;
            for (const auto& li : orderToLineItems.at(o.o_orderkey)) {
                if (suppToNation.count(li.l_suppkey) == 0) continue;
                int suppNation = suppToNation.at(li.l_suppkey);
                if (suppNation != nationKey) continue;

                std::string nationName = nationNames.at(nationKey);
                double revenue = li.l_extendedprice * (1 - li.l_discount);
                localMap[nationName] += revenue;
            }
        }
    }

    std::lock_guard<std::mutex> lock(mtx);
    for (const auto& [nation, rev] : localMap)
        localRevenue[nation] += rev;
}

int main(int argc, char* argv[]) {
    std::ofstream log("output.txt");

    log << "Reading data files...\n";

    auto regions = readRegions("data/region.tbl");
    auto nations = readNations("data/nation.tbl");
    auto customers = readCustomers("data/customer.tbl");
    auto orders = readOrders("data/orders.tbl");
    auto lineitems = readLineItems("data/lineitem.tbl");
    auto suppliers = readSuppliers("data/supplier.tbl");

    log << "Data loading complete:\n";
    log << "Regions: " << regions.size() << "\n";
    log << "Nations: " << nations.size() << "\n";
    log << "Customers: " << customers.size() << "\n";
    log << "Orders: " << orders.size() << "\n";
    log << "LineItems: " << lineitems.size() << "\n";
    log << "Suppliers: " << suppliers.size() << "\n\n";

    std::unordered_map<int, std::string> regionNames;
    for (const auto& r : regions) regionNames[r.r_regionkey] = r.r_name;

    std::unordered_map<int, int> nationToRegion;
    std::unordered_map<int, std::string> nationNames;
    for (const auto& n : nations) {
        nationToRegion[n.n_nationkey] = n.n_regionkey;
        nationNames[n.n_nationkey] = n.n_name;
    }

    std::unordered_map<int, int> custToNation;
    for (const auto& c : customers) custToNation[c.c_custkey] = c.c_nationkey;

    std::unordered_map<int, int> suppToNation;
    for (const auto& s : suppliers) suppToNation[s.s_suppkey] = s.s_nationkey;

    std::unordered_map<int, std::vector<LineItem>> orderToLineItems;
    for (const auto& li : lineitems)
        orderToLineItems[li.l_orderkey].push_back(li);

    int numThreads = 4;
    std::vector<std::thread> threads;
    std::unordered_map<std::string, double> nationRevenue;

    int chunkSize = orders.size() / numThreads;

    log << "Starting multithreaded processing...\n";
    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < numThreads; ++i) {
        int s = i * chunkSize;
        int e = (i == numThreads - 1) ? orders.size() : (i + 1) * chunkSize;

        threads.emplace_back(processChunk,
            std::ref(orders),
            std::ref(custToNation),
            std::ref(nationNames),
            std::ref(nationToRegion),
            std::ref(regionNames),
            std::ref(orderToLineItems),
            std::ref(suppToNation),
            "ASIA", "1993-01-01", "1996-01-01",
            std::ref(nationRevenue), s, e
        );
    }

    for (auto& t : threads) t.join();

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> diff = end - start;

    log << "Processing complete.\n\n";
    log << "Sorting results...\n";

    std::vector<std::pair<std::string, double>> sortedResults(nationRevenue.begin(), nationRevenue.end());
    std::sort(sortedResults.begin(), sortedResults.end(),
              [](auto& a, auto& b) { return a.second > b.second; });

    log << "Final Revenue by Nation (in ASIA, 1995):\n";
    for (const auto& [nation, rev] : sortedResults)
        log << nation << ": " << rev << "\n";

    log << "\nExecution Time: " << diff.count() << " seconds\n";

    std::cout << "Results written to output.txt\n";
    return 0;
}
