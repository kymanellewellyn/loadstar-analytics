# **Conformed Dimensions**

All dimensions follow:

* **Surrogate Key (PK)**: warehouse join key  
* **Business Key (BK)**: source identifier

---

## **Core Dimensions**

### **`dim_truck`**

* `truck_sk` (PK)  
* `truck_id` (BK)  
* attributes: vin, make, model, year, capacity, type, status

---

### **`dim_site`**

* `site_sk` (PK)  
* `site_id` (BK)  
* attributes: name, type, city, state, lat/long, region

---

### **`dim_driver`**

* `driver_sk`  
* `driver_id`  
* attributes: name, license, employment status

---

### **`dim_customer`**

* `customer_sk`  
* `customer_id`  
* attributes: type, billing terms, contract type

---

### **`dim_pricing_model`**

* `pricing_model_sk`  
* `pricing_model_id`  
* attributes: name, rate unit

---

### **`dim_job`**

* `job_sk`  
* `job_id`

---

### **`dim_date`**

* `date_key`  
* calendar attributes

---

### **Maintenance Dimensions (Objective 3\)**

* `dim_failure_type`  
* `dim_vendor`  
* `dim_downtime_cause`  
* `dim_weather_condition`

