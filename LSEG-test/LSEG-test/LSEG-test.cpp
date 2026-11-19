// LSEG-test.cpp : This file contains the 'main' function. Program execution begins and ends there.
//

#include <algorithm>
#include <format>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <queue>
#include <sstream>
#include <string>
#include <thread>

struct MarketDataRow {
    int index = 0;
    std::string symbol = "";
    double price = 0.0;
    int quantity = 0;
};

std::queue<MarketDataRow> marketDataQueue;
std::mutex queueMutex;
std::condition_variable queueCondition;
bool producerFinished = false;

// Parse a csv line into a market data row struct.
MarketDataRow parseStringToMarketDataRow(std::string str) {
    std::stringstream ss(str);
    std::string token;
    MarketDataRow result;

    std::getline(ss, token, ',');
    result.index = std::stoi(token, nullptr, 16);

    std::getline(ss, token, ',');
    result.symbol = token;
    
    std::getline(ss, token, ',');
    result.price = std::stod(token);

    std::getline(ss, token, ',');
    result.quantity = std::stoi(token);

    return result;
}

// Sort first by price, ascending; then by index, descending.
bool compareRows(const MarketDataRow& a, const MarketDataRow& b) {
    return a.price == b.price ? a.index > b.index : a.price < b.price;
}

// Write a vector of market data rows to a CSV file.
int writeMarketDataRows(std::string symbol, std::vector<MarketDataRow> rows) {
    // Sort rows
    std::sort(rows.begin(), rows.end(), compareRows);

    // Check that the file opened successfully.
    std::ofstream outputFile(symbol + ".csv");
    if (!outputFile.is_open()) {
        std::cerr << "Error opening file!\n";
        return 1;
    }

    // Write each row as a line
    for (const auto& row : rows) {
        outputFile << std::format("{:.2f},{},{}\n", row.price, row.quantity, row.index);
    }

    return 0;
}

void producer(std::string filepath) {
    std::ifstream file(filepath);
    std::string line;
    int rowsRead = 0;


    // Read the CSV file one line at a time
    while (std::getline(file, line)) {

        MarketDataRow row;

        try {
            // Parse the line into a struct
            row = parseStringToMarketDataRow(line);
            ++rowsRead;
        }
        catch (const std::exception& e) {
            std::cout << std::format("Error--could not parse the line: \"{}\"\nSkipping...\n", line);
            continue;
        }
        

        // Add the struct to the shared queue
        {
            std::lock_guard<std::mutex> lock(queueMutex);
            marketDataQueue.push(row);
        }

        // Tell the consumer there's data available.
        queueCondition.notify_one();
    }

    // Flag that we have finished the file.
    {
        std::lock_guard<std::mutex> lock(queueMutex);
        producerFinished = true;
    }

    std::cout << std::format("Total Rows {}\n", rowsRead);

    // Tell the consumer we're done.
    queueCondition.notify_one();

}

void consumer() {
    std::map<std::string, std::vector<MarketDataRow>> groupedRows;

    while (true) {
        MarketDataRow marketDataRow;

        {
            std::unique_lock<std::mutex> lock(queueMutex);
            queueCondition.wait(lock,
                [] {
                    return !marketDataQueue.empty() || producerFinished;
                });

            if (marketDataQueue.empty() && producerFinished) {
                break;
            }

            marketDataRow = marketDataQueue.front();
            marketDataQueue.pop();
        }

        // Add the row to the map
        if (!groupedRows.contains(marketDataRow.symbol)) {
            groupedRows[marketDataRow.symbol] = {};
        }
        groupedRows[marketDataRow.symbol].push_back(marketDataRow);
    }

    // We've finished reading in the data, so now we can write our output files.

    for (const auto& [symbol, rows] : groupedRows) {
        // Sort rows by price
        writeMarketDataRows(symbol, rows);
        std::cout << std::format("{} Rows: {}\n", symbol, rows.size());
    }

    return;
}


int main(int argc, char* argv[])
{
    if (argc != 2) {
        std::cout << "Error: use with one command line arg, the filepath of the input file.\n";
    }

    std::thread producerThread(producer, argv[1]);
    std::thread consumerThread(consumer);

    producerThread.join();
    consumerThread.join();

    return 0;
}

// Run program: Ctrl + F5 or Debug > Start Without Debugging menu
// Debug program: F5 or Debug > Start Debugging menu

// Tips for Getting Started: 
//   1. Use the Solution Explorer window to add/manage files
//   2. Use the Team Explorer window to connect to source control
//   3. Use the Output window to see build output and other messages
//   4. Use the Error List window to view errors
//   5. Go to Project > Add New Item to create new code files, or Project > Add Existing Item to add existing code files to the project
//   6. In the future, to open this project again, go to File > Open > Project and select the .sln file
