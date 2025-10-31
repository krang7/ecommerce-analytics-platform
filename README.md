Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
## Data Model

### Dimensional Model Structure

#### Staging Layer (Views)
- `stg_customers` - Cleaned customer data
- `stg_orders` - Cleaned order data
- `stg_items_ordered` - Cleaned order items
- `stg_products` - Cleaned product data
- `stg_sellers` - Cleaned seller data
- `stg_order_payment` - Cleaned payment data
- `stg_name_translation` - Product category translations

#### Dimension Layer (Tables)
- `dim_customers` - Customer master with lifetime metrics and RFM segmentation
- `dim_products` - Product master with categories and sales performance
- `dim_sellers` - Seller master with location and performance tiers
- `dim_time` - Date dimension for time-based analysis

### Key Metrics
- **Customer Lifetime Value**: Total amount spent by each customer
- **Customer Segmentation**: RFM-based (Recency, Frequency, Monetary)
- **Product Performance**: Sales volume categorization
- **Seller Performance**: Order volume and price tier classification

### How to Run
```bash
# Run all models
dbt run

# Run only dimensions
dbt run --select marts

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```
