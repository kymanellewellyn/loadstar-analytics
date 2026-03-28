# **Architecture Overview**

### **Objective 1:  Batch Analytics**

* trips  
* jobs  
* pricing  
* revenue

  ### **Objective 3: Event Processing**

* failures  
* repairs  
* downtime  
  ---

  ## **Architecture Pattern**

  ### **Bronze**

* raw JSON events   
* immutable  
* replayable

  ### **Silver**

* cleaned, normalized  
* schema enforced  
* deduplicated  
* entities extracted

  ### **Gold**

* dimensional models  
* business KPIs  
* optimized for analytics  
  ---

  ## **Key Technologies**

* Delta Lake: ACID, schema evolution  
* Structured Streaming: incremental event ingestion  
* Unity Catalog: data governance  
* Lakeflow: orchestration  
  ---

# **AI Layer (High Value Add)**

### **Agent A — Ops Analyst**

* analyzes downtime  
* detects anomalies  
* suggests root causes

  ### **Agent B — Exec KPI Narrator**

* summarizes revenue \+ margin  
* generates reports

  ### **Agent C — Data Steward**

* manages definitions  
* ensures consistency  
* updates documentation


