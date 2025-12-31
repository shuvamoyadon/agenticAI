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
