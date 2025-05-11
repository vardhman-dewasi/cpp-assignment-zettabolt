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
    std::unordered_map<std::string, double> localMap; // Thread-local revenue map

    // Iterate through assigned orders
    for (int i = start; i < end; ++i) {
        const Order& o = orders[i];     //Current order

        // Filter orders based on date range
        if (o.o_orderdate >= startDate && o.o_orderdate < endDate) {
            int custKey = o.o_custkey;
            auto custIt = custToNation.find(custKey);
            if (custIt == custToNation.end()) continue;
            int nationKey = custIt->second;

            // Check if customer belongs to correct region
            auto regionIt = nationToRegion.find(nationKey);
            if (regionIt == nationToRegion.end()) continue;
            int regionKey = regionIt->second;

            auto regionNameIt = regionNames.find(regionKey);
            if (regionNameIt == regionNames.end()) continue;
            const std::string& regionName = regionNameIt->second;

            if (regionName != regionFilter) continue;

            // Get associated line items for the order
            auto lineItemIt = orderToLineItems.find(o.o_orderkey);
            if (lineItemIt == orderToLineItems.end()) continue;
            
            for (const auto& li : lineItemIt->second) {
                auto suppIt = suppToNation.find(li.l_suppkey);
                if (suppIt == suppToNation.end()) continue;

                // Supplier and customer must be from same nation
                int suppNation = suppIt->second;
                if (suppNation != nationKey) continue;

                // Calculate revenue and accumulate in local map
                auto nationNameIt = nationNames.find(nationKey);
                if (nationNameIt == nationNames.end()) continue;
                const std::string& nationName = nationNameIt->second;

                double revenue = li.l_extendedprice * (1 - li.l_discount);
                localMap[nationName] += revenue;
            }
        }
    }

    // Merge localMap into shared result under mutex
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
    log << "Suppliers: " << suppliers.size() << "\n\n";

    // Preprocess lookup maps for fast access during query

    // Region ID → Region Name
    std::unordered_map<int, std::string> regionNames;
    for (const auto& r : regions)
        regionNames[r.r_regionkey] = r.r_name;

    // Nation ID → Region ID and Nation ID → Nation Name
    std::unordered_map<int, int> nationToRegion;
    std::unordered_map<int, std::string> nationNames;
    for (const auto& n : nations) {
        nationToRegion[n.n_nationkey] = n.n_regionkey;
        nationNames[n.n_nationkey] = n.n_name;
    }

    // Customer ID → Nation ID
    std::unordered_map<int, int> custToNation;
    for (const auto& c : customers)
        custToNation[c.c_custkey] = c.c_nationkey;

    // Supplier ID → Nation ID
    std::unordered_map<int, int> suppToNation;
    for (const auto& s : suppliers)
        suppToNation[s.s_suppkey] = s.s_nationkey;

    // Order ID → List of LineItems
    std::unordered_map<int, std::vector<LineItem>> orderToLineItems;
    for (const auto& li : lineitems)
        orderToLineItems[li.l_orderkey].push_back(li);


    //------ Multithreading setup ------

    int numThreads = args.numThreads;  //Number of threads for launch
    std::vector<std::thread> threads;  // Vector to store thread objects
    std::unordered_map<std::string, double> nationRevenue;  // Shared result map that will hold revenue aggregated by nation

    int chunkSize = orders.size() / numThreads;  // Size of each chunk to be processed by a thread

    log << "Launching " << numThreads << " threads...\n";

    auto start = std::chrono::high_resolution_clock::now();   // Record start time for performance measurement

    // Loop to create and launch each thread
    for (int i = 0; i < numThreads; ++i) {
        int s = i * chunkSize;  // Start index for this thread

        int e = (i == numThreads - 1) ? orders.size() : (i + 1) * chunkSize;      // end index

        // Launch a new thread to process a specific range of orders using processChunk()
        threads.emplace_back(
            processChunk,                     // Function to run
            std::ref(orders),                 // Orders vector (shared across threads)
            std::ref(custToNation),           // Customer → Nation lookup (shared)
            std::ref(nationNames),            // Nation ID → Name (shared)
            std::ref(nationToRegion),         // Nation → Region lookup (shared)
            std::ref(regionNames),            // Region ID → Name (shared)
            std::ref(orderToLineItems),       // Order ID → Line items (shared)
            std::ref(suppToNation),           // Supplier → Nation lookup (shared)
            args.regionName,
            args.startDate,
            args.endDate,
            std::ref(nationRevenue),          // Shared result map (protected by mutex)
            s,
            e
        );
    }

    for (auto& t : threads) t.join();       // Wait for all threads to finish

    //End time for performance measurement
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> diff = end - start;

    log << "Processing complete.\n\n";

    // Sort results by descending revenue
    std::vector<std::pair<std::string, double>> sortedResults(nationRevenue.begin(), nationRevenue.end());
    std::sort(sortedResults.begin(), sortedResults.end(),
              [](auto& a, auto& b) { return a.second > b.second; });

    // Output the final revenue per nation
    log << "Final Revenue by Nations:\n";
    for (const auto& [nation, rev] : sortedResults)
        log << nation << ": " << rev << "\n";

    log << "\nExecution Time: " << diff.count() << " seconds\n";

    std::cout << "Results written to: "<< args.outputPath <<"\n";
    return 0;
}
