# Retail-Data-Analysis---Apache-Kafka-and-Apache-Spark-Streaming
# Purpose:
Big data tools improve business performance by enabling companies to analyse trends and current consumer behavioural patterns and offer better and more customised products.For the purpose of this project, you have been tasked with computing various Key Performance Indicators (KPIs) for an e-commerce company, RetailCorp Inc. 

# Aim:
You have been provided real-time sales data of the company across the globe. The data contains information related to the invoices of orders placed by customers all around the world.Each order’s invoice has been represented in a JSON format. The sample data looks like this.

{
  "invoice_no": 154132541653705,
  "country": "United Kingdom",
  "timestamp": "2020-09-18 10:55:23",
  "type": "ORDER",
  "items": [
    {
      "SKU": "21485",
      "title": "RETROSPOT HEART HOT WATER BOTTLE",
      "unit_price": 4.95,
      "quantity": 6
    },
    {
      "SKU": "23499",
      "title": "SET 12 VINTAGE DOILY CHALK",
      "unit_price": 0.42,
      "quantity": 2
    }
  ]  
}

As you can see, the data contains the following information:
- Invoice number: Identifier of the invoice
- Country: Country where the order is placed
- Timestamp: Time at which the order is placed
- Type: Whether this is a new order or a return order
- SKU (Stock Keeping Unit): Identifier of the product being ordered
- Title: Name of the product is ordered
- Unit price: Price of a single unit of the product
- Quantity: Quantity of the product being ordered


Perform the following tasks:
1. Reading the sales data from the Kafka server
2. Preprocessing the data to calculate additional derived columns such as total_cost etc
3. Calculating the time-based KPIs and time and country-based KPIs
4. Storing the KPIs (both time-based and time- and country-based) for a 10-minute interval into separate JSON files for further analysis
These KPIs will be calculated on a tumbling window of one minute on orders across the globe.
