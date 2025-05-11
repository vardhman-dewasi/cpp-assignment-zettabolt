#ifndef CLI_PARSER_H 
#define CLI_PARSER_H

#include <string>

// Struct to store the parsed command-line arguments.
// Each member corresponds to one of the parameters needed for TPCH Query 5.
struct CLIArgs {
    std::string regionName;
    std::string startDate; 
    std::string endDate;
    int numThreads;
    std::string dataDir;
    std::string outputPath; 
};

// Function declaration to parse CLI arguments into a CLIArgs struct
// This function is defined in cli_parser.cpp
CLIArgs parseCLIArgs(int argc, char* argv[]);

#endif // CLI_PARSER_H
