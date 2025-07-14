-- follow this guide to renew the key-pair 
-- https://docs.snowflake.com/en/user-guide/key-pair-auth

--step 1: run the follow commands to create both encrypted private and public keys
-- private key: openssl genrsa 2048 | openssl pkcs8 -topk8 -v2 des3 -inform PEM -out rsa_key.p8
-- public key: openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
-- store both files in a secure location

-- step 2: create the users
CREATE OR REPLACE USER SPN_CACHE_REPLICATION
  TYPE=SERVICE
  DEFAULT_ROLE='ACCOUNTADMIN'
  DEFAULT_WAREHOUSE='COMPUTE_WH'
  COMMENT='SPN used to Caché database replication';
SHOW USERS;

-- step 3: adjust the authentication method and granting privilege
GRANT MODIFY PROGRAMMATIC AUTHENTICATION METHODS ON USER SPN_CACHE_REPLICATION
    TO ROLE ACCOUNTADMIN
GRANT ROLE ACCOUNTADMIN TO USER SPN_CACHE_REPLICATION;

-- step 4: copy public key and set it in the recent user 
ALTER USER SPN_CACHE_REPLICATION SET 
    RSA_PUBLIC_KEY='MIIBIjA...';

-- step 5: run this command to get the key
DESC USER SPN_CACHE_REPLICATION
  ->> SELECT SUBSTR(
        (SELECT "value" FROM $1
           WHERE "property" = 'RSA_PUBLIC_KEY_FP'),
        LEN('SHA256:') + 1) AS key;
-- output: yVc...D8U=

-- run this command in local machine
-- openssl rsa -pubin -in rsa_key.pub -outform DER | openssl dgst -sha256 -binary | openssl enc -base64 
-- output: yVc...D8U=

-- if both keys are the same, key-pair authentication is ready to be used