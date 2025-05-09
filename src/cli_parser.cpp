#include "../include/cli_parser.h" // Include the header that defines the CLIArgs struct and function declaration
#include <iostream>                // For input/output operations
#include <stdexcept>              // For exception handling (used in std::stoi)

CLIArgs parseCLIArgs(int argc, char* argv[]) {
    // Check if the correct number of arguments is passed
    if (argc != 7) {
        std::cerr << "Usage: " << argv[0] << " <REGION> <START_DATE> <END_DATE> <NUM_THREADS> <DATA_DIR> <OUTPUT_PATH>\n";
        std::cerr << "Example: ./tpch_q5 ASIA 1995-01-01 1996-01-01 4 ./data/ output.txt\n";
        exit(1); // Exit the program with error code 1 if the number of arguments is incorrect
    }

    CLIArgs args; // Create an instance of CLIArgs to store parsed values

    // Assign values from command-line arguments to the struct
    args.regionName = argv[1];       // REGION
    args.startDate = argv[2];        // START_DATE
    args.endDate = argv[3];          // END_DATE
    args.numThreads = std::stoi(argv[4]); // Convert thread count string to integer

    // Ensure the data directory path ends with '/' to simplify file path joining later
    args.dataDir = argv[5];
    if (args.dataDir.back() != '/')
        args.dataDir += '/';

    args.outputPath = argv[6];       // OUTPUT_PATH

    return args; // Return the parsed arguments
}
