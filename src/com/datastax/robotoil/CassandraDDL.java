package com.datastax.robotoil;

// This is a data generator for Packathon.
// This program accepts a json file generated from the BestBuy API.
// http://bestbuyapis.github.io/bby-query-builder/#/productSearch
// Get a free API key to access the API query builder.
// Match the columns in the product_catalog in the generated json file.
// This program will:
//      load the json. 100 products.
//      generate 100 client UUIDs in a 0-100 range.
//      randomly submit product orders on behalf of the clients.
//      the quantity of products ordered at any one time is between 1-100.
//      this program accepts as arguments: host timeToSleepInMilliseconds pathToJsonFile

import com.datastax.driver.core.*;
import com.datastax.driver.core.utils.UUIDs;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileReader;
import java.util.*;

public class CassandraDDL {

    private Cluster cluster;
    private static Session session;

    //connect
    public void connect(String node) {

        cluster = Cluster.builder()
                .addContactPoint(node)
                .build();

        // just for testing - make sure it connected to cluster
        Metadata metadata = cluster.getMetadata();
        System.out.printf("Connected to cluster: " + metadata.getClusterName() + "\n");

        for (Host host : metadata.getAllHosts()) {
            System.out.printf("Datatacenter: " + host.getDatacenter() + "; Host: " + host.getAddress() + "; Rack: " + host.getRack() + "\n");
        }

        session = cluster.connect();
    }

    //create schema
    public void createSchema() {
        session.execute("drop keyspace if exists retail_ks;");

        session.execute("create keyspace if not exists retail_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1' };");

        session.execute("create table if not exists retail_ks.product_catalog ("
                + "color ascii,"
                + "description ascii,"
                + "image ascii,"
                + "in_store_availability Boolean,"
                + "manufacturer ascii,"
                + "model_number ascii,"
                + "name ascii,"
                + "regular_price double,"
                + "short_description ascii,"
                + "sku ascii,"
                + "thumbnail_image ascii,"
                + "upc ascii,"
                + "PRIMARY KEY (sku)"
                + ");");
        System.out.println("table created: retail_ks.product_catalog");

        session.execute("create type if not exists retail_ks.order_line ("
                + "sku ascii,"
                + "product_name ascii,"
                + "quantity int,"
                + "unit_price double,"
                + "total_price double"
                + ");");
        System.out.println("table created: retail_ks.order_line");

        session.execute("create table if not exists retail_ks.orders ("
                + "customer_id uuid,"
                + "order_id timeuuid,"
                + "date timestamp,"
                + "order_lines list<frozen<order_line>>,"
                + "PRIMARY KEY (customer_id, order_id)"
                + ")"
                + "WITH CLUSTERING ORDER BY (order_id DESC);");
        System.out.println("table created: retail_ks.orders");

        session.execute("create table if not exists retail_ks.top50_selling_products ("
                + "sku ascii,"
                + "sale_count int,"
                + "PRIMARY KEY ((sku, sale_count))"
                + ");");
        System.out.println("table created: retail_ks.top50_selling_products");

        session.execute("create table if not exists retail_ks.top_selling_products_by_customer ("
                + "customer_id uuid,"
                + "sku ascii,"
                + "sale_count int,"
                + "PRIMARY KEY (customer_id, sale_count)"
                + ")"
                + "WITH CLUSTERING ORDER BY (sale_count DESC);");
        System.out.println("table created: retail_ks.top_selling_products_by_customer");
    }

    public Integer loadJson(String fileName) {
        PreparedStatement statement1 = session.prepare("INSERT into retail_ks.product_catalog ("
                + "color,"
                + "description,"
                + "image,"
                + "in_store_availability,"
                + "manufacturer,"
                + "model_number,"
                + "name,"
                + "regular_price,"
                + "short_description,"
                + "sku,"
                + "thumbnail_image,"
                + "upc)"
                + "values (?,?,?,?,?,?,?,?,?,?,?,?) USING ttl 172800;");

        JSONParser parser = new JSONParser();
        int rowCounter = 0; // Track number of products

        try {
            Object obj = parser.parse(new FileReader(fileName));
            System.out.println("Input file =" + fileName);

            JSONObject jsonObject = (JSONObject) obj;
            JSONArray itemList = (JSONArray) jsonObject.get("products");
            Iterator<String> item = itemList.iterator();
            while (item.hasNext()) {
                Object itemObject = item.next();
                JSONObject itemJson = (JSONObject) itemObject;

                String color = (String) itemJson.get("color");
                String description = (String) itemJson.get("description");
                String image = (String) itemJson.get("image");
                Boolean in_store_availability = (Boolean) itemJson.get("inStoreAvailability");
                String manufacturer = (String) itemJson.get("manufacturer");
                String model_number = (String) itemJson.get("modelNumber");
                String name = (String) itemJson.get("name");
                Double regular_price = (Double) itemJson.get("regularPrice");
                String short_description = (String) itemJson.get("shortDescription");
                Long skuin = (Long) itemJson.get("sku");
                String sku = Long.toString(skuin);
                String thumbnail_image = (String) itemJson.get("thumbnailImage");
                String upc = (String) itemJson.get("upc");

                BoundStatement boundStatement1 = new BoundStatement(statement1);
                boundStatement1.setString("color", color);
                //boundStatement1.setString("description", description);
                boundStatement1.setString("description", " "); // All null
                boundStatement1.setString("image", image);
                boundStatement1.setBool("in_store_availability", in_store_availability);
                boundStatement1.setString("manufacturer", manufacturer);
                boundStatement1.setString("model_number", model_number);
                boundStatement1.setString("name", name);
                boundStatement1.setDouble("regular_price", regular_price);
                boundStatement1.setString("short_description", short_description);
                boundStatement1.setString("short_description", short_description);
                boundStatement1.setString("sku", sku);
                boundStatement1.setString("thumbnail_image", thumbnail_image);
                boundStatement1.setString("upc", upc);

                session.execute(boundStatement1);

                rowCounter++;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        //System.out.println("Rows inserted: " + rowCounter);
        return (rowCounter);
    }


    public void placeOrders() {

        ResultSet results = session.execute("SELECT sku, name, regular_price FROM retail_ks.product_catalog;");
        int numberOfProducts = results.getAvailableWithoutFetching();
        String[][] productList = new String[numberOfProducts][3];
        // Build product array
        int counter = 0;
        for (Row row : results) {
            productList[counter][0] = row.getString("sku");
            productList[counter][1] = row.getString("name");
            double regular_price = row.getDouble("regular_price");
            productList[counter][2] = Double.toString(regular_price);

            //System.out.println("counter = "+counter+ " : sku ="+productList[counter][0]+ " : name = "+productList[counter][1]+ " : regular_price = "+regular_price );
            counter++;
        }

        int sleepTime = 10;
        System.out.println("Sending orders every " + sleepTime + " milliseconds");
        Random rand = new Random();

        for (int i = 0; i < 100001; i++) {
            if (i % 1000 == 0) {
                System.out.println("Orders placed = " + i);
            }
            // Slow it down
            try {
                Thread.sleep(sleepTime);                 //1000 milliseconds is one second.
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }


            // Create a customer. There will be 100.
            int randomC = rand.nextInt((800 - 700) + 1) + 700;
            String randomCustomer = Integer.toString(randomC);
            String stringUUID = "a4a70900-24e1-11df-8924-001ff3591".concat(randomCustomer);
            UUID customer_id = UUID.fromString(stringUUID);
            //System.out.println("Random customer id = " + customer_id);

            // Create a quantity. May be between 1-100.
            int randomQuantity = rand.nextInt(99) + 1;
            if (randomQuantity > 100) {
                randomQuantity = 99;
            }

            // Select a random product
            // Create a quantity. May be between 1-100.
            int randomProduct = rand.nextInt(numberOfProducts) + 1;
            if (randomProduct > 99) {
                randomProduct = 99;
            }
            //System.out.println("Random product number = "+randomProduct);

            // Total price
            double price = Double.parseDouble(productList[randomProduct][2]);
            double total_price = randomQuantity * price;

            // Insert order
            ResultSet execute = session.execute("insert into retail_ks.orders ("
                    + "customer_id,"
                    + "order_id,"
                    + "date,"
                    + "order_lines"
                    + ")"
                    + "values("
                    + customer_id + ","
                    + UUIDs.timeBased() + ","
                    + "dateof(now()),"
                    + "[{"
                    + "sku:'" + productList[randomProduct][0] + "',"
                    + "product_name:'" + productList[randomProduct][1] + "',"
                    + "quantity:" + randomQuantity + ","
                    + "unit_price:" + productList[randomProduct][2] + ","
                    + "total_price:" + total_price
                    + "}]"
                    + ");");
        }
    }


    public void close() {
        session.close();
        cluster.close();
    }

    public static void main(String[] args) {

        // Cluster IPs
        String host = "127.0.0.1";
        if (args.length > 0) {
            host = args[0];
        }

        // How many milliseconds to sleep in between orders
        int sleepTime = 10;
        if (args.length > 1) {
            sleepTime = Integer.parseInt(args[1]);
        }

        // Location of the json file to load
        String fileName = "/Users/ebergman-RMBP15/Documents/packethon/products.json";
        if (args.length > 2) {
            fileName = args[2];
        }

        int numberOfProducts = 0;

        CassandraDDL client = new CassandraDDL();
        client.connect(host);
        client.createSchema();
        numberOfProducts = client.loadJson(fileName);
        System.out.println("Products loaded = " + numberOfProducts);
        client.placeOrders();
        client.close();

    }
}
