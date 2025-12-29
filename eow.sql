INSERT INTO `playground-s-11-443f35ef.test.T_PCT_GCP_FIN_PRODUCT_SRC` 
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
  PARSE_JSON("""
  [
    {
      "AttributeName__c": "Billing_Model",
      "Attribute_Category__c": "Billing",
      "Attribute_Definition__c": "0tjc0000001ddAAA",
      "Attribute_Value__c": "Not Selected",
      "Client_External_Id__c": "AccExtIdHC601225",
      "Configured_Product__c": "0QLbc000002E4x3GAC",
      "CreatedDate": "2025-12-22T18:15:02.000Z",
      "Grouper_ID__c": "TestingGrouper1225",
      "Id": "a09bc00000C9dkrAAB",
      "LastModifiedBy": {
        "Name": {
          "value": "PCT GCP Integration User"
        }
      },
      "LastModifiedDate": "2025-12-22T18:15:02.000Z",
      "Name": "PCH-151528",
      "Product_Association_ID": "a01bc00000UuJxzAAF",
      "line_of_business": "Caremark"
    },
    {
      "AttributeName__c": "Notes_Paid_For_By_Other_Selected",
      "Attribute_Category__c": "Billing",
      "Attribute_Definition__c": "0tjc0000001fAAA",
      "Attribute_Value__c": null,
      "Client_External_Id__c": "AccExtIdHC601225",
      "Configured_Product__c": "0QLbc000002E4x3GAC",
      "CreatedDate": "2025-12-22T18:15:02.000Z",
      "Grouper_ID__c": "TestingGrouper1225",
      "Id": "a09bc00000C9dksAAB",
      "LastModifiedBy": {
        "Name": {
          "value": "PCT GCP Integration User"
        }
      },
      "LastModifiedDate": "2025-12-22T18:15:02.000Z",
      "Name": "PCH-151529",
      "Product_Association_ID": "a01bc00000UuJxzAAF",
      "line_of_business": "Caremark"
    }
  ]
  """)
);
