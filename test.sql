## change the Attribute_Category__c and check
INSERT INTO `playground-s-11-d5e75d75.test.T_PCT_GCP_FIN_PRODUCT_CONFIGURATION`
(
  ID,
  Product_Association_ID,
  line_of_business,
  Grouper_val,
  CreatedDate,
  LastModifiedDate,
  inserted_time,
  updated_time,
  indicator_flag,
  Payload
)
VALUES
(
  'a09bc00000C9dkrAAB',
  'a01bc00000UuJxzAAF',
  'Caremark',
  'TestingGrouper1225',
  TIMESTAMP('2025-12-22T18:15:02.000Z'),
  TIMESTAMP('2025-12-22T18:15:02.000Z'),
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP(),
  'N',
  JSON '''
  [
    {
      "AttributeName__c": "Billing Model",
      "Attribute_Category__c": "Client Contract",
      "Attribute_Definition__c": "0tjbce000001dAAA",
      "Attribute_Value__c": "PMPM (per member per month)",
      "Client_External_Id__c": "785469",
      "Configured_Product__c": "0QLbce00002FMALG4",
      "CreatedDate": "2025-12-24T13:05:58.000Z",
      "Grouper_ID__c": "0QLbce00002FMALG4",
      "Id": "a09bc00000CWD3AA5",
      "LastModifiedBy": {
        "Name": {
          "value": "Anupama Retnadev"
        }
      },
      "LastModifiedDate": "2025-12-24T13:05:58.000Z",
      "Name": "PCH-151915",
      "Product_Association_ID": "a01bce0000ZLnvAAF",
      "line_of_business": "Third Party"
    },
    {
      "AttributeName__c": "Notes Paid For By Other Selected",
      "Attribute_Category__c": "Client Contract",
      "Attribute_Definition__c": "0tjbce000001fAAA",
      "Attribute_Value__c": null,
      "Client_External_Id__c": "785469",
      "Configured_Product__c": "0QLbce00002FMALG4",
      "CreatedDate": "2025-12-24T13:05:58.000Z",
      "Grouper_ID__c": "0QLbce00002FMALG4",
      "Id": "a09bc00000CWDKAA5",
      "LastModifiedBy": {
        "Name": {
          "value": "Anupama Retnadev"
        }
      },
      "LastModifiedDate": "2025-12-24T13:05:58.000Z",
      "Name": "PCH-151916",
      "Product_Association_ID": "a01bce0000ZLnvAAF",
      "line_of_business": "Third Party"
    },
    {
      "AttributeName__c": "Contracted LOB Sub Category",
      "Attribute_Category__c": "Client Contract",
      "Attribute_Definition__c": "0tjbce000001gAAA",
      "Attribute_Value__c": "Commercial Fully Insured;FTI;Med EGP Group;Med EGP Individual",
      "Client_External_Id__c": "AccExtInde2323",
      "Configured_Product__c": "0QLbce00002GYFugAO",
      "CreatedDate": "2025-12-26T16:00:04.000Z",
      "Grouper_ID__c": "wegwe",
      "Id": "a09bc00000CF1GAA1",
      "LastModifiedBy": {
        "Name": {
          "value": "PCT GCP Integration User"
        }
      },
      "LastModifiedDate": "2025-12-26T16:00:04.000Z",
      "Name": "PCH-152069",
      "Product_Association_ID": "a01bce00004dqaAAB",
      "line_of_business": "Caremark"
    },
    {
      "AttributeName__c": "Notes_CoBranding_Level_Specific_Explanation",
      "Attribute_Category__c": "Client Contract",
      "Attribute_Definition__c": "0tjbce000002EJAA",
      "Attribute_Value__c": null,
      "Client_External_Id__c": "AccExtInde2323",
      "Configured_Product__c": "0QLbce00002GYFugAO",
      "CreatedDate": "2025-12-26T16:00:04.000Z",
      "Grouper_ID__c": "wegwe",
      "Id": "a09bc00000CF1GAA4",
      "LastModifiedBy": {
        "Name": {
          "value": "PCT GCP Integration User"
        }
      },
      "LastModifiedDate": "2025-12-26T16:00:04.000Z",
      "Name": "PCH-152070",
      "Product_Association_ID": "a01bce00004dqaAAB",
      "line_of_business": "Caremark"
    }
  ]
  '''
);



######  another insert for sencond source table

INSERT INTO `playground-s-11-53ad36ac.test.T_PCT_GCP_FIN_PRODUCT_CONFIGURATION_1`
(
  ID,
  Product_Association_ID,
  line_of_business,
  Grouper_val,
  CreatedDate,
  LastModifiedDate,
  inserted_time,
  updated_time,
  indicator_flag,
  Payload
)
VALUES
(
  'a09bc00000C9dkrAAB',
  'a01bc00000UuJxzAAF',
  'Caremark',
  'TestingGrouper1225',
  TIMESTAMP('2025-12-22T18:15:02.000Z'),
  TIMESTAMP('2025-12-22T18:15:02.000Z'),
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP(),
  'N',
  JSON '''
  [
    {
      "Activity_Rules__r": [
        {
          "Activity_Rule_Months__c": 12,
          "CreatedDate": "2025-12-12T18:35:11.000Z",
          "Effective_Date__c": "2026-01-01",
          "Expiry_Date__c": null,
          "Id": "a0Ebc000003y6COEAY",
          "LastModifiedDate": "2025-12-12T18:35:11.000Z",
          "Name": "AR-00006",
          "Program_Rule__c": "a0Dbc000007ehv3EAA"
        }
      ],
      "CCNLs__r": [
        {
          "CreatedDate": "2025-12-12T18:43:51.000Z",
          "Id": "a0Gbc000005mLFVEAU",
          "IsActive__c": true,
          "LastModifiedDate": "2025-12-12T18:43:51.000Z",
          "Name": "CCNL-0002",
          "News_Letter__c": "English",
          "Plan_Code__c": "BH191",
          "Plan_Name__c": "BAKERHUGHES",
          "Plan_Type__c": "Super Payer",
          "Program_Rule__c": "a0Dbc000007ehv3EAA",
          "Start_Date__c": "2026-01-01"
        },
        {
          "CreatedDate": "2025-12-12T18:44:29.000Z",
          "Id": "a0Gbc000005mSxXEAE",
          "IsActive__c": true,
          "LastModifiedDate": "2025-12-12T18:44:29.000Z",
          "Name": "CCNL-0003",
          "News_Letter__c": "Spanish",
          "Plan_Code__c": "BH191",
          "Plan_Name__c": "BAKERHUGHES",
          "Plan_Type__c": "Super Payer",
          "Program_Rule__c": "a0Dbc000007ehv3EAA",
          "Start_Date__c": "2026-01-01"
        }
      ],
      "Call_Sheets__r": [
        {
          "All_Calls_Limit__c": 20,
          "CreatedDate": "2025-12-12T21:30:11.000Z",
          "First_Call_Limit__c": 56,
          "Id": "a00bc000001RUW5EAO",
          "LastModifiedDate": "2025-12-12T21:30:11.000Z",
          "Name": "CS-0002",
          "Program_Rule__c": "a0Dbc000007ehv3EAA",
          "Required_Calls__c": 6
        }
      ],
      "Email_Campaigns__r": [
        {
          "Case_Id__c": "1234",
          "Client__c": "001bc00000TXV4MAAX",
          "Condition__c": null,
          "CreatedDate": "2025-12-12T21:20:49.000Z",
          "Email__c": "test@xyz.com",
          "HMN_First_Name__c": null,
          "Id": "a0Mbc000001AUDxEAO",
          "Language__c": "English",
          "LastModifiedDate": "2025-12-12T21:20:49.000Z",
          "MRN__c": "2314561",
          "Member_First_Name__c": null,
          "My_Chart_Activation__c": true,
          "Name": "EMC-0002",
          "Others__c": null,
          "Patient_Access_Code__c": null,
          "Program_Rule__c": "a0Dbc000007ehv3EAA"
        }
      ],
      "Engagement_Rules__r": [
        {
          "CreatedDate": "2025-12-12T18:39:33.000Z",
          "Effective_Date__c": "2026-01-01",
          "Engagement_Rule_Months__c": 20,
          "Expiry_Date__c": null,
          "Id": "a0Fbc000002MjJEAS",
          "LastModifiedDate": "2025-12-12T18:39:33.000Z",
          "Name": "ER-0002",
          "Program_Rule__c": "a0Dbc000007ehv3EAA"
        }
      ],
      "Fulfillments__r": [
        {
          "CreatedDate": "2025-12-12T19:08:30.000Z",
          "End_Date__c": "2026-03-31",
          "Id": "a0Ibc000006ZjTREA",
          "Language__c": "English",
          "LastModifiedDate": "2025-12-12T19:08:30.000Z",
          "Letter_Template__c": "MC103",
          "Letter_Type__c": "Self Directed",
          "Material_Name__c": "Self Directed Letter",
          "Material_Suite_Code__c": "MCCSG",
          "Name": "F-0002",
          "Program_Rule__c": "a0Dbc000007ehv3EAA",
          "Seq__c": 1,
          "Start_Date__c": "2026-01-01",
          "Type__c": "Letter"
        }
      ],
      "HEDISs__r": [
        {
          "Age_From__c": 20,
          "Age_To__c": 50,
          "CreatedDate": "2025-12-12T17:44:22.000Z",
          "Effective_Date__c": "2026-01-01",
          "Expiry_Date__c": "2026-03-31",
          "Gender__c": "Female",
          "Id": "a02bc00000GNvD7AAL",
          "LastModifiedDate": "2025-12-12T18:31:12.000Z",
          "Metric_Code__c": "BCS",
          "Metric_Description__c": "Breast Cancer Screening",
          "Name": "H-00008",
          "Program_Rule__c": "a0Dbc000007ehv3EAA",
          "Sub_Metric__c": null
        }
      ],
      "Health_Optimizers__r": [
        {
          "Id": "a0Nbc000005vjojEAA",
          "LastModifiedDate": "2025-12-12T21:25:49.000Z",
          "Name": "HO-0002",
          "Payer_CTE__c": "2000",
          "Program_Rule__c": "a0Dbc000007ehv3EAA",
          "Required_Calls__c": null
        }
      ],
      "IVRs__r": [
        {
          "Campaign_Name__c": "SLAM",
          "CreatedDate": "2025-12-12T19:22:00.000Z",
          "Id": "a0Kbc000007SR2sEAG",
          "IsPayerParticipating__c": true,
          "IsSuperPayerParticipating__c": true,
          "LastModifiedDate": "2025-12-12T19:22:00.000Z",
          "Name": "IVR-0005",
          "Program_Rule__c": "a0Dbc000007ehv3EAA"
        }
      ],
      "Program_Flips__r": [
        {
          "Create_Date__c": "2026-01-01",
          "CreatedDate": "2025-12-12T21:47:52.000Z",
          "Enable_Flag__c": false,
          "Flip_Type__c": "RaretoCTA",
          "Id": "a07bc000004ScpdEAC",
          "LastModifiedDate": "2025-12-12T21:47:52.000Z",
          "Name": "PF-0001",
          "New_Payer__c": "Healthcare Account 3",
          "New_Super_Payer__c": "Healthcare Account",
          "Update_Date__c": "2026-03-31"
        }
      ],
      "SMS_Campaigns__r": [
        {
          "Appt_Date_Time__c": "2026-01-01T20:00:00.000Z",
          "Cell_Phone__c": "1234567890",
          "Condition_ID__c": "13",
          "CreatedDate": "2025-12-12T20:20:55.000Z",
          "Id": "a0Lbc000004sXWFEA2",
          "Language__c": "English",
          "LastModifiedDate": "2025-12-12T20:20:55.000Z",
          "MRN__c": "2314561",
          "Name": "SMS-0003",
          "Program_Rule__c": "a0Dbc000007ehv3EAA",
          "Run_Date__c": "12122025"
        }
      ],
      "Id": "a0Dbc000007ehv3EAA",
      "LastModifiedDate": "2025-12-12T17:43:34.000Z",
      "Name": "Test Program Rule",
      "line_of_business": "Aetna"
    }
  ]
  '''
);
