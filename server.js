const express = require("express");
const bodyParser = require("body-parser");
const { forEach, isNil, get } = require("lodash");
const fs = require("fs");
const app = express();
const csvWriter = require("csv-writer");
const Client = require("ssh2-sftp-client");
const path = require("path");

// const csvWriter = require("csv-writer");
// // File paths
// const transactionsFile = path.resolve(`${__dirname}/../Downloads/problem1/transactions.json`);
// const outputCsvFile = path.join(__dirname, "filtered_transactions.csv");
// const ignoredTxtFile = path.join(__dirname, "ignored_transactions.csv");
// const allCountriesFile = path.resolve(`${__dirname}/../Downloads/problem1/countries_all.txt`);
// const blacklistFile = path.resolve(`${__dirname}/../Downloads/problem1/countries_blacklisted.txt`);

// // Function to get the country name from an address
// const extractCountry = (address) => {
// 	if (isNil(address)) return null;
// 	const parts = address.split(",").map((part) => part.trim());
// 	return parts[parts.length - 1];
// };

// const csvFileHolder = async (TxtFile) => {
// 	const writer1 = csvWriter.createObjectCsvWriter({
// 		path: TxtFile,
// 		header: [
// 			{ id: "transaction_id", title: "transaction_id" },
// 			{ id: "sender_account", title: "sender_account" },
// 			{ id: "sender_routing_number", title: "sender_routing_number" },
// 			{ id: "sender_name", title: "sender_name" },
// 			{ id: "sender_address", title: "sender_address" },
// 			{ id: "receiver_account", title: "receiver_account" },
// 			{ id: "receiver_routing_number", title: "receiver_routing_number" },
// 			{ id: "receiver_name", title: "receiver_name" },
// 			{ id: "receiver_address", title: "receiver_address" },
// 			{ id: "transaction_amount", title: "transaction_amount" },
// 		],
// 	});
// };

// app.post("/sanctionCheckValidationCheck", async (req, res) => {
// 	try {
// 		// Load data
// 		const transactions = JSON.parse(fs.readFileSync(transactionsFile, "utf8")).transactions;
// 		const allCountries = new Set(
// 			fs
// 				.readFileSync(allCountriesFile, "utf8")
// 				.split("\n")
// 				.map((c) => c.trim())
// 		);
// 		const blacklistedCountries = new Set(
// 			fs
// 				.readFileSync(blacklistFile, "utf8")
// 				.split("\n")
// 				.map((c) => c.trim())
// 		);
// 		// Prepare CSV writer
// 		const writer = csvFileHolder(outputCsvFile);
// 		const writer1 = csvFileHolder(ignoredTxtFile);

// 		const blacklistedTransactions = [];
// 		const ignoredTransactions = [];
// 		// Process each transaction
// 		for (const transaction of transactions) {
// 			const senderCountry = extractCountry(get(transaction, "sender_address", null));
// 			const receiverCountry = extractCountry(get(transaction, "receiver_address", null));

// 			if (isNil(senderCountry) || isNil(receiverCountry)) {
// 				ignoredTransactions.push(transaction);
// 			} else if (blacklistedCountries.has(senderCountry) || blacklistedCountries.has(receiverCountry)) {
// 				blacklistedTransactions.push(transaction);
// 			} else if (!allCountries.has(senderCountry) || !allCountries.has(receiverCountry)) {
// 				ignoredTransactions.push(transaction);
// 			}
// 		}
// 		// Write blacklisted transactions to CSV
// 		if (blacklistedTransactions.length > 0) {
// 			await writer.writeRecords(blacklistedTransactions);
// 			console.log("Blacklisted transactions saved to CSV.");
// 		} else {
// 			console.log("No blacklisted transactions found.");
// 		}
// 		// Write ignored transactions to TXT
// 		if (ignoredTransactions.length > 0) {
// 			await writer1.writeRecords(ignoredTransactions);
// 			console.log("Ignored transactions saved to CSV.");
// 		} else {
// 			console.log("No Ignored transactions found.");
// 		}
// 	} catch (error) {
// 		console.error("Error processing transactions:", error);
// 	}
// });

// const PORT = process.env.PORT || 3003;
// app.listen(PORT, () => {
// 	console.log(`Rule management API is running on port ${PORT}`);
// });

// File paths
const ruleEngineFile = path.resolve(`${__dirname}/rules.json`);
// Helper function to load JSON file
const loadJson = (filePath) => JSON.parse(fs.readFileSync(filePath, "utf8"));

// Helper function to extract country
const extractCountry = (address) => {
	if (isNil(address)) return null;
	const parts = address.split(",").map((part) => part.trim());
	return parts[parts.length - 1];
};

// Evaluate a single condition
const evaluateCondition = (transaction, condition, dataSources) => {
	const { field, operator, value } = condition;
	let transactionValue = get(transaction, field, null);
	transactionValue = extractCountry(transactionValue);
	switch (operator) {
		case "in":
			return dataSources[value].has(transactionValue);
		case "not_in":
			return !dataSources[value].has(transactionValue);
		// case "is_null":
		// 	return isNil(transactionValue);
	}
};

// Evaluate all/any conditions for a rule
const evaluateConditions = (transaction, conditions, dataSources) => {
	const { all, any } = conditions;

	if (all) {
		return all.every((condition) => evaluateCondition(transaction, condition, dataSources));
	}
	if (any) {
		return any.some((condition) => evaluateCondition(transaction, condition, dataSources));
	}
	return false;
};

// Process rules
const processRules = async (transactions, rules, dataSources) => {
	for (const rule of rules) {
		if (!rule.enabled) continue;

		// Prepare CSV writer for this rule's action
		const writer = csvWriter.createObjectCsvWriter({
			path: rule.action.file,
			header: Object.keys(transactions[0] || {}).map((key) => ({
				id: key,
				title: key,
			})),
		});

		const matchingTransactions = transactions.filter((transaction) =>
			evaluateConditions(transaction, rule.conditions, dataSources)
		);

		if (matchingTransactions.length > 0) {
			await writer.writeRecords(matchingTransactions);
			console.log(`${rule.action.log_message} Saved ${matchingTransactions.length} records to ${rule.action.file}`);
		}
	}
};

app.post("/sanctionCheckValidationCheck", async (req, res) => {
	try {
		// Load data
		const ruleEngine = loadJson(ruleEngineFile);

		// Prepare data sources
		const dataSources = {};
		for (const [key, filePath] of Object.entries(ruleEngine.dataSources)) {
			if (key)
				dataSources[key] = new Set(
					fs
						.readFileSync(filePath, "utf8")
						.split("\n")
						.map((c) => c.trim())
				);
		}
		const transactions = JSON.parse(fs.readFileSync(ruleEngine.inputPath.transactions, "utf8")).transactions;
		// Process rules
		await processRules(transactions, ruleEngine.rules, dataSources);
		console.log("Processing completed.");
		res.status(200).end("Done");
	} catch (error) {
		console.error("Error processing transactions:", error);
		res.status(500).end("Not Working fine");
	}
});

const PORT = process.env.PORT || 3003;
app.listen(PORT, () => {
	console.log(`Rule management API is running on port ${PORT}`);
});

