--The following is code for creating a lead funnel in Looker using a common leads infastructure. The logic can be easily modified
--for other BI tools. 

Suppose your lead stage table contains the following: 
Lead ID | Stage    | Stage_Updated_Timestamp 
1001    | New      |  01 - 01 - 2019 
1001    | Qlfd     |  01 - 02 - 2019 
1001    | Apprv    |  01 - 05 - 2019 
1001    | Active   |  01 - 06 - 2019 
1001    | Rjctd    |  




--FIRST -- create a new table with every lead, and the first time it moved through a given stage. It's important to use the min 
--function to avoid double counting. 

SELECT 
  lead_id, 
  CASE WHEN stage = 'New' THEN MIN(Stage_Updated_Timestamp)ELSE NULL END as new_ts 
  CASE WHEN stage = 'Qlfd' THEN MIN(Stage_Updated_Timestamp)ELSE NULL END as qualified_ts   
  CASE WHEN stage = 'Rjctd' THEN MIN(Stage_Updated_Timestamp)ELSE NULL END as rejected_ts
  CASE WHEN stage = 'Apprv' THEN MIN(Stage_Updated_Timestamp)ELSE NULL END as approved_ts  
  CASE WHEN stage = 'Active' THEN MIN(Stage_Updated_Timestamp)ELSE NULL END as active_ts 
FROM lead_stage_table 


--SECOND -- implement the "is following" logic that is key to funnels. Here is an easy way to think about it: 
Stage  | Lead Count  | Explanation 
 New   |    100      | 100 leads are new or "following" in the stages Qlfd, Apprv, Active, Rjctd
Qlfd   |    80       | Between New and Qlfd only 80 leads make it to Qlfd or "following" in the stages Apprv, Active, Rjctd
Apprv  |    75       | Between Qlfd and Apprv only 75 leads make it to Apprv or "following" in the stages Active, Rjctd
Active |    50       | Between Apprv and Active only 50 leads make it to Active or "following" in Rjctd
Rjctd  |    50       | Simply the number of leads that have been rejected 


--In Looker, the implementation of the logic is just to create a few measures in the format: 
  
    dimension: lead_id {
    description: "Lead ID"
    primary_key: yes
    type: number
    sql: ${TABLE}.lead_id ;;
    hidden: yes
  }
  
  
  dimension: qualified_or_following {
    description: "Lead Qualified or Following"
    hidden: yes 
    type: string
    sql: CASE WHEN ${TABLE}.qualified_ts IS NOT NULL 
              OR ${TABLE}.approved_ts IS NOT NULL
              OR ${TABLE}.active_ts IS NOT NULL
              OR ${TABLE}.rejected_ts IS NOT NULL
              THEN TRUE ELSE FALSE END 
              ;;
  }

  measure: n_qualified_or_following {
    label: "# Leads Qualified or following"
    type: count
    sql: ${lead_id} ;;
    filters: {
      field: qualified_or_following
      value: "true"
    }
  }
 
 
 --THIRD -- Create a measure for each stage and you'll have your funnel! 
 
 
 
 
 
 
 
 
