# **Data Pipeline Maintenance Plan**

This document outlines the ownership, on-call schedule, and runbooks for the five core data pipelines.


## **Pipeline Ownership**

To ensure clear responsibility and knowledge sharing, we'll assign primary and secondary owners to each pipeline. This balances the workload and prevents knowledge silos.


<table>
  <tr>
   <td><strong>Pipeline</strong>
   </td>
   <td><strong>Business Area</strong>
   </td>
   <td><strong>Primary Owner</strong>
   </td>
   <td><strong>Secondary Owner</strong>
   </td>
  </tr>
  <tr>
   <td>Unit-Level Profit
   </td>
   <td>Profit
   </td>
   <td><strong>Engineer A</strong>
   </td>
   <td>Engineer B
   </td>
  </tr>
  <tr>
   <td>Aggregate Profit
   </td>
   <td>Profit
   </td>
   <td><strong>Engineer B</strong>
   </td>
   <td>Engineer A
   </td>
  </tr>
  <tr>
   <td>Daily Growth
   </td>
   <td>Growth
   </td>
   <td><strong>Engineer C</strong>
   </td>
   <td>Engineer D
   </td>
  </tr>
  <tr>
   <td>Aggregate Growth
   </td>
   <td>Growth
   </td>
   <td><strong>Engineer D</strong>
   </td>
   <td>Engineer C
   </td>
  </tr>
  <tr>
   <td>Aggregate Engagement
   </td>
   <td>Engagement
   </td>
   <td><strong>Engineer A</strong>
   </td>
   <td>Engineer C
   </td>
  </tr>
</table>



## **On-Call Schedule**

This rotating schedule ensures fair distribution of on-call responsibilities, including weekends and holidays. The on-call engineer is the first point of contact for any pipeline failures.

**Rotation:** Engineer A -> Engineer B -> Engineer C -> Engineer D -> (repeat)

**Schedule:**



* **Q3 2025:**
    * August 4 - August 10: **Engineer A**
    * August 11 - August 17: **Engineer B**
    * August 18 - August 24: **Engineer C**
    * August 25 - August 31: **Engineer D**
    * September 1 - September 7: **Engineer A** (Includes Labor Day)
    * ...and so on.

**Holiday Coverage:** The on-call rotation continues as normal during holidays. The engineer on-call during a holiday can swap with another team member if they need coverage. All swaps must be agreed upon by both parties and communicated to the team.


## **Runbooks for Investor-Facing Pipelines**

These runbooks are the first line of defense for troubleshooting critical pipeline failures.


### **1. Aggregate Profit Pipeline Runbook**



* **Primary Owner:** Engineer B
* **Secondary Owner:** Engineer A
* **Upstream Owners:** Finance Department (for raw profit data), Sales Team (for transaction data).
* **Common Issues:**
    * Late or missing raw data from the Finance team's systems.
    * Incorrect currency conversion rates applied.
    * Schema changes in the upstream transaction database.
    * Bugs in the transformation logic that calculates aggregate profit.
    * Data duplication from source systems.
* **Critical Downstream Owners:** Investor Relations Team, Executive Leadership Team.
* **SLAs and Agreements:** For any pipeline failures, the on-call engineer is expected to acknowledge **critical (P1) issues within 1 hour** and achieve **resolution within 24 hours**.


### **2. Aggregate Growth Pipeline Runbook**



* **Primary Owner:** Engineer D
* **Secondary Owner:** Engineer C
* **Upstream Owners:** Marketing Team (for user acquisition data), Product Team (for new user sign-up events).
* **Common Issues:**
    * Changes to tracking parameters in marketing campaigns.
    * Delays in receiving user sign-up event data from the product database.
    * Incorrect attribution of user acquisition channels.
    * API failures from third-party marketing analytics platforms.
    * Seasonal spikes or dips in data that could be mistaken for errors.
* **Critical Downstream Owners:** Investor Relations Team, Marketing Leadership, Executive Leadership Team.
* **SLAs and Agreements:** For any pipeline failures, the on-call engineer is expected to acknowledge **critical (P1) issues within 1 hour** and achieve **resolution within 24 hours**.


### **3. Aggregate Engagement Pipeline Runbook**



* **Primary Owner:** Engineer A
* **Secondary Owner:** Engineer C
* **Upstream Owners:** Product Analytics Team (for user interaction events), Frontend Engineering Team (for instrumentation).
* **Common Issues:**
    * New app releases that change or break existing event tracking.
    * Missing or malformed user interaction event data.
    * A/B testing platforms interfering with standard event collection.
    * Incorrectly defined "active user" logic in the transformation script.
    * Data pipeline latency causing delays in reporting.
* **Critical Downstream Owners:** Investor Relations Team, Product Leadership, Executive Leadership Team.
* **SLAs and Agreements:** For any pipeline failures, the on-call engineer is expected to acknowledge **critical (P1) issues within 1 hour** and achieve **resolution within 24 hours**.
