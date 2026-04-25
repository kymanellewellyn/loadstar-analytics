# **Objective 1: Increase Revenue Per Truck**

## How We Achieve This

* Reduce costs  
* Select higher-margin jobs  
* Optimize pricing model selection  
* Improve dispatching efficiency  
* Reduce idle time  
* Increase loads per truck per day

---

## Key Metrics

### Revenue \+ Margin

* Revenue per truck per day  
* Margin per truck per day  
* Revenue per trip  
* Margin per job

### Cost Efficiency

* Cost per mile  
* Cost per load  
* Cost per truck per day  
* Fuel cost per mile

### Pricing Effectiveness

* Margin by pricing model  
* Revenue variance (estimated vs actual)  
* Margin by job type

# **KPI-to-Model:**

This section defines how business KPIs map directly to fact tables.

Each fact table strictly separates:

* **Grain-defining columns**  
* **Measurement columns**  
* **Analytical (dimension) columns**

---

## **Fact Definitions**

### **`fact_trip`**

**Grain:** one trip

**Keys**

* `trip_id` (degenerate)  
* `date_key`  
* `truck_sk`  
* `driver_sk`  
* `customer_sk`  
* `job_sk`  
* `pickup_site_sk`  
* `drop_site_sk`  
* `pricing_model_sk`

**Measures**

* `trip_revenue`  
* `trip_cost`  
* `trip_miles`

**Supports KPIs**

* Revenue per trip  
* Cost per mile  
* Revenue per mile

---

### **`fact_truck_day`**

**Grain:** one truck per day

**Keys**

* `date_key`  
* `truck_sk`  
* `primary_site_sk`

**Measures**

* `revenue`  
* `cost`  
* `margin`  
* `trip_count`  
* `miles_driven`

**Supports KPIs**

* Revenue per truck per day  
* Margin per truck per day

---

### **`fact_job`**

**Grain:** one job

**Keys**

* `job_sk`  
* `customer_sk`  
* `pricing_model_sk`  
* `job_type_sk`  
* `created_date_key`  
* `completed_date_key`

**Measures**

* `job_revenue`  
* `job_cost`  
* `job_margin`

**Supports KPIs**

* Margin per job  
* Revenue per job

![][image1]

[image1]: <data:image/png;base64,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>