#include "../include/tpch_structs.h"
#include "../include/file_reader.h"
#include "../include/cli_parser.h"

#include <bits/stdc++.h>
#include <mutex>
#include <thread>
#include <chrono>
#include <fstream>

std::mutex mtx; // Mutex to synchronize access to shared `nationRevenue` map across threads

// Function to process a chunk of orders and accumulate revenue per nation
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

    // Iterate through assigned orders
    for (int i = start; i < end; ++i) {
        const Order& o = orders[i];

        // Filter orders based on date range
        if (o.o_orderdate >= startDate && o.o_orderdate < endDate) {
            int custKey = o.o_custkey;
            if (custToNation.count(custKey) == 0) continue;
            int nationKey = custToNation.at(custKey);

            // Check if customer belongs to correct region
            if (nationToRegion.count(nationKey) == 0) continue;
            int regionKey = nationToRegion.at(nationKey);
            std::string regionName = regionNames.at(regionKey);
            if (regionName != regionFilter) continue;

            // Get associated line items for the order
            if (orderToLineItems.count(o.o_orderkey) == 0) continue;
            for (const auto& li : orderToLineItems.at(o.o_orderkey)) {
                if (suppToNation.count(li.l_suppkey) == 0) continue;

                // Supplier and customer must be from same nation
                int suppNation = suppToNation.at(li.l_suppkey);
                if (suppNation != nationKey) continue;

                std::string nationName = nationNames.at(nationKey);
                double revenue = li.l_extendedprice * (1 - li.l_discount);
                localMap[nationName] += revenue;
            }
        }
    }

    // Lock and update shared result map
    std::lock_guard<std::mutex> lock(mtx);
    for (const auto& [nation, rev] : localMap)
        localRevenue[nation] += rev;
}

int main(int argc, char* argv[]) {
    //--- Parse CLI Arguments ---
    CLIArgs args = parseCLIArgs(argc, argv);

    std::ofstream log(args.outputPath);

    log << "Reading data files from: " << args.dataDir << "\n";

    // --- Load Data ---
    auto regions   = readRegions(args.dataDir + "region.tbl");
    auto nations   = readNations(args.dataDir + "nation.tbl");
    auto customers = readCustomers(args.dataDir + "customer.tbl");
    auto orders    = readOrders(args.dataDir + "orders.tbl");
    auto lineitems = readLineItems(args.dataDir + "lineitem.tbl");
    auto suppliers = readSuppliers(args.dataDir + "supplier.tbl");

    log << "Data loading complete.\n";

    // Log data loading stats
    log << "Regions: " << regions.size() << "\n";
    log << "Nations: " << nations.size() << "\n";
    log << "Customers: " << customers.size() << "\n";
    log << "Orders: " << orders.size() << "\n";
    log << "LineItems: " << lineitems.size() << "\n";
    log << "Suppliers: " << suppliers.size() << "\n";

    // Preprocess lookup maps for fast access during query
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

    // Group lineitems by order key for fast lookup
    std::unordered_map<int, std::vector<LineItem>> orderToLineItems;
    for (const auto& li : lineitems)
        orderToLineItems[li.l_orderkey].push_back(li);

    // Multithreading setup
    int numThreads = args.numThreads;
    std::vector<std::thread> threads;
    std::unordered_map<std::string, double> nationRevenue;
    int chunkSize = orders.size() / numThreads;

    log << "Launching " << numThreads << " threads...\n";
    auto start = std::chrono::high_resolution_clock::now();

    // Launch threads to process chunks of the orders vector
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
            args.regionName, args.startDate, args.endDate,
            std::ref(nationRevenue), s, e
        );
    }

    // Wait for all threads to finish
    for (auto& t : threads) t.join();

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> diff = end - start;

    log << "Processing complete.\n\n";
    log << "Sorting results...\n";

    // Sort results by descending revenue
    std::vector<std::pair<std::string, double>> sortedResults(nationRevenue.begin(), nationRevenue.end());
    std::sort(sortedResults.begin(), sortedResults.end(),
              [](auto& a, auto& b) { return a.second > b.second; });

    // Output the final revenue per nation
    log << "Final Revenue by Nation:\n";
    for (const auto& [nation, rev] : sortedResults)
        log << nation << ": " << rev << "\n";

    log << "\nExecution Time: " << diff.count() << " seconds\n";

    std::cout << "Results written to: "<< args.outputPath <<"\n";
    return 0;
}
