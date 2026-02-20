PostgreSQL CDC Replication to Snowflake via OpenFlow (SPCS with Private Link)

  Overview

  This guide covers end-to-end setup for CDC replication from AWS RDS PostgreSQL to Snowflake using OpenFlow running in Snowpark Container Services (SPCS) with Private Link connectivity.

  ────────────────────────────────────────

  Part 1: PostgreSQL (AWS RDS) Side Configuration

  1.1 Verify Logical Replication Settings

  Connect to your RDS PostgreSQL instance and verify these settings:

    -- Check wal_level (must be 'logical')
    SHOW wal_level;

    -- Check max_replication_slots (should be >= number of connectors)
    SHOW max_replication_slots;

    -- Check max_wal_senders (should be >= max_replication_slots)
    SHOW max_wal_senders;

  For AWS RDS: These are set via Parameter Groups. If not already configured:

  1. Create or modify a Parameter Group with:

     • rds.logical_replication = 1
     • max_replication_slots = 10 (or higher)
     • max_wal_senders = 10 (or higher)
  2. Apply the Parameter Group to your RDS instance
  3. Reboot the RDS instance (required for wal_level change)

  1.2 Create Publication for Target Tables

    -- Create a publication for specific tables
    CREATE PUBLICATION openflow_publication FOR TABLE
        schema_name.table1,
        schema_name.table2,
        schema_name.table3;

    -- OR for all tables in a schema
    CREATE PUBLICATION openflow_publication FOR ALL TABLES IN SCHEMA schema_name;

    -- Verify the publication
    SELECT * FROM pg_publication;
    SELECT * FROM pg_publication_tables WHERE pubname = 'openflow_publication';

  1.3 Create Dedicated Replication User

    -- Create replication user
    CREATE USER openflow_replication WITH PASSWORD 'your_secure_password';

    -- Grant replication privilege
    GRANT rds_replication TO openflow_replication;

    -- Grant SELECT on target tables
    GRANT USAGE ON SCHEMA schema_name TO openflow_replication;
    GRANT SELECT ON ALL TABLES IN SCHEMA schema_name TO openflow_replication;

    -- Grant SELECT on future tables (optional)
    ALTER DEFAULT PRIVILEGES IN SCHEMA schema_name
        GRANT SELECT ON TABLES TO openflow_replication;

  1.4 Verify Tables Have Primary Keys

  CDC requires primary keys on all replicated tables:

    -- Find tables without primary keys
    SELECT t.table_schema, t.table_name
    FROM information_schema.tables t
    LEFT JOIN information_schema.table_constraints tc
        ON t.table_schema = tc.table_schema
        AND t.table_name = tc.table_name
        AND tc.constraint_type = 'PRIMARY KEY'
    WHERE t.table_schema = 'schema_name'
        AND t.table_type = 'BASE TABLE'
        AND tc.constraint_name IS NULL;

  ────────────────────────────────────────

  Part 2: AWS Networking Configuration for Private Link

  2.1 Architecture Overview

    ┌─────────────────────────────────────────────────────────────────┐
    │                         Customer AWS VPC                         │
    │  ┌─────────────────┐         ┌─────────────────────────────┐   │
    │  │   RDS PostgreSQL│◄────────│  Network Load Balancer      │   │
    │  │   (Primary)      │         │  (Internal, TCP 5432)       │   │
    │  └─────────────────┘         └──────────────┬──────────────┘   │
    │                                              │                   │
    │                              ┌───────────────┴───────────────┐  │
    │                              │  VPC Endpoint Service          │  │
    │                              │  (PrivateLink)                 │  │
    │                              └───────────────┬───────────────┘  │
    └──────────────────────────────────────────────┼──────────────────┘
                                                   │
                        PrivateLink Connection     │
                                                   ▼
    ┌─────────────────────────────────────────────────────────────────┐
    │                      Snowflake SPCS                              │
    │  ┌─────────────────────────────────────────────────────────┐   │
    │  │               OpenFlow Runtime                           │   │
    │  │  (PostgreSQL CDC Connector)                              │   │
    │  └─────────────────────────────────────────────────────────┘   │
    └─────────────────────────────────────────────────────────────────┘

  2.2 Create Target Group for RDS

    # Get your RDS endpoint IP (resolve the DNS)
    nslookup your-rds-instance.xxx.us-east-1.rds.amazonaws.com

    # Create Target Group (via AWS CLI)
    aws elbv2 create-target-group \
        --name openflow-rds-postgres-tg \
        --protocol TCP \
        --port 5432 \
        --vpc-id vpc-xxxxxxxx \
        --target-type ip \
        --health-check-protocol TCP \
        --health-check-port 5432

    # Register RDS IP as target
    aws elbv2 register-targets \
        --target-group-arn arn:aws:elasticloadbalancing:region:account:targetgroup/openflow-rds-postgres-tg/xxx \
        --targets Id=<RDS_IP_ADDRESS>,Port=5432

  2.3 Create Network Load Balancer

    # Create internal NLB
    aws elbv2 create-load-balancer \
        --name openflow-postgres-nlb \
        --type network \
        --scheme internal \
        --subnets subnet-xxx subnet-yyy subnet-zzz

    # Create listener
    aws elbv2 create-listener \
        --load-balancer-arn arn:aws:elasticloadbalancing:region:account:loadbalancer/net/openflow-postgres-nlb/xxx \
        --protocol TCP \
        --port 5432 \
        --default-actions Type=forward,TargetGroupArn=arn:aws:elasticloadbalancing:region:account:targetgroup/openflow-rds-postgres-tg/xxx

  2.4 Create VPC Endpoint Service

    # Create Endpoint Service
    aws ec2 create-vpc-endpoint-service-configuration \
        --network-load-balancer-arns arn:aws:elasticloadbalancing:region:account:loadbalancer/net/openflow-postgres-nlb/xxx \
        --acceptance-required \
        --private-dns-name postgres.yourcompany.internal

    # Get the Service Name (needed for Snowflake)
    aws ec2 describe-vpc-endpoint-service-configurations \
        --query 'ServiceConfigurations[*].[ServiceId,ServiceName]'

    # Output example: com.amazonaws.vpce.us-east-1.vpce-svc-xxxxxxxxxxxxxxxxx

  2.5 Allow Snowflake Principal

  Get Snowflake's AWS principal for your account:

    -- Run in Snowflake
    SELECT SYSTEM$GET_PRIVATELINK_CONFIG();

  This returns JSON including the aws_vpce_id (Snowflake's VPC Endpoint ID). Add permission:

    # Allow Snowflake to connect
    aws ec2 modify-vpc-endpoint-service-permissions \
        --service-id vpce-svc-xxxxxxxxxxxxxxxxx \
        --add-allowed-principals arn:aws:iam::SNOWFLAKE_AWS_ACCOUNT:root

  2.6 Configure Security Groups

  Ensure your RDS security group allows inbound from the NLB:

    # Allow NLB health checks and traffic (NLB uses IPs from its subnets)
    aws ec2 authorize-security-group-ingress \
        --group-id sg-xxxxxxxx \
        --protocol tcp \
        --port 5432 \
        --cidr <NLB_SUBNET_CIDR>

  ────────────────────────────────────────

  Part 3: Snowflake Side Configuration

  3.1 Create Private Link Endpoint in Snowflake

    -- Create the private endpoint to AWS VPC Endpoint Service
    USE ROLE ACCOUNTADMIN;

    CREATE SECURITY INTEGRATION privatelink_postgres
        TYPE = PRIVATE_LINK
        ENABLED = TRUE
        PRIVATE_LINK_ENDPOINT = 'com.amazonaws.vpce.us-east-1.vpce-svc-xxxxxxxxxxxxxxxxx';

    -- Verify the integration
    DESCRIBE INTEGRATION privatelink_postgres;

  3.2 Accept the VPC Endpoint Connection (AWS Side)

  After creating the Snowflake integration, accept the pending connection in AWS:

    # List pending connections
    aws ec2 describe-vpc-endpoint-connections \
        --filters Name=service-id,Values=vpce-svc-xxxxxxxxxxxxxxxxx

    # Accept the connection
    aws ec2 accept-vpc-endpoint-connections \
        --service-id vpce-svc-xxxxxxxxxxxxxxxxx \
        --vpc-endpoint-ids vpce-xxxxxxxxxxxxxxxxx

  3.3 Create Network Rule and External Access Integration

    -- Step 1: Create Network Rule for the Private Link endpoint
    USE ROLE SECURITYADMIN;

    CREATE NETWORK RULE postgres_cdc_network_rule
        TYPE = HOST_PORT
        MODE = EGRESS
        VALUE_LIST = ('<privatelink-endpoint-dns>:5432');

    -- Note: The privatelink endpoint DNS format is typically:
    -- vpce-xxxxxxxxx.vpce-svc-yyyyyyyyy.us-east-1.vpce.amazonaws.com

    -- Step 2: Verify the Network Rule
    DESCRIBE NETWORK RULE postgres_cdc_network_rule;

    -- Step 3: Create External Access Integration
    CREATE EXTERNAL ACCESS INTEGRATION postgres_cdc_eai
        ALLOWED_NETWORK_RULES = (postgres_cdc_network_rule)
        ENABLED = TRUE
        COMMENT = 'External Access Integration for OpenFlow PostgreSQL CDC via PrivateLink';

    -- Step 4: Verify the EAI
    DESCRIBE INTEGRATION postgres_cdc_eai;

    -- Step 5: Grant USAGE to the OpenFlow Runtime Role
    -- Get the runtime role from OpenFlow Control Plane or:
    SHOW OPENFLOW DATA PLANE INTEGRATIONS;

    -- Grant to the runtime role
    GRANT USAGE ON INTEGRATION postgres_cdc_eai TO ROLE <OPENFLOWRUNTIMEROLE_xxx>;

  3.4 Attach EAI to OpenFlow Runtime (UI Required)

  1. Navigate to Snowsight → Data → OpenFlow
  2. Select your Runtime
  3. Click the "..." menu → "External access integrations"
  4. Select postgres_cdc_eai from the dropdown
  5. Click Save

  3.5 Verify Network Connectivity from SPCS

  Before deploying the connector, validate connectivity. Use the network test flow described in the OpenFlow documentation, or deploy a test processor:

    -- Check OpenFlow runtime logs for connectivity tests
    SELECT * FROM <OPENFLOW_EVENTS_TABLE>
    WHERE TIMESTAMP > DATEADD('hour', -1, CURRENT_TIMESTAMP())
    ORDER BY TIMESTAMP DESC
    LIMIT 100;

  ────────────────────────────────────────

  Part 4: OpenFlow PostgreSQL CDC Connector Configuration

  4.1 Prerequisites Checklist

  Before proceeding, confirm you have collected:

  ┌───────────────────────────┬─────────────────────────────────────────────────────┬───────────┐
  │ Item                      │ Value                                               │ Collected │
  ├───────────────────────────┼─────────────────────────────────────────────────────┼───────────┤
  │ PostgreSQL Connection URL │ jdbc:postgresql://<privatelink-dns>:5432/<database> │ [ ]       │
  ├───────────────────────────┼─────────────────────────────────────────────────────┼───────────┤
  │ PostgreSQL Username       │ openflow_replication                                │ [ ]       │
  ├───────────────────────────┼─────────────────────────────────────────────────────┼───────────┤
  │ PostgreSQL Password       │ (sensitive)                                         │ [ ]       │
  ├───────────────────────────┼─────────────────────────────────────────────────────┼───────────┤
  │ Publication Name          │ openflow_publication                                │ [ ]       │
  ├───────────────────────────┼─────────────────────────────────────────────────────┼───────────┤
  │ Tables to Replicate       │ e.g., public.orders,public.customers                │ [ ]       │
  ├───────────────────────────┼─────────────────────────────────────────────────────┼───────────┤
  │ Destination Database      │ Snowflake database name                             │ [ ]       │
  ├───────────────────────────┼─────────────────────────────────────────────────────┼───────────┤
  │ Snowflake Role            │ Role with CREATE SCHEMA privileges                  │ [ ]       │
  ├───────────────────────────┼─────────────────────────────────────────────────────┼───────────┤
  │ Snowflake Warehouse       │ Warehouse for processing                            │ [ ]       │
  └───────────────────────────┴─────────────────────────────────────────────────────┴───────────┘

  4.2 Discover OpenFlow Infrastructure

  First, set up your OpenFlow session:

    # Check OpenFlow deployments
    snow sql -c myconnection -q "SHOW OPENFLOW DATA PLANE INTEGRATIONS;"

    # Get runtime details
    snow sql -c myconnection -q "SHOW OPENFLOW RUNTIMES;"

  4.3 Set Up nipyapi Profile

    # Create nipyapi profile for your runtime
    nipyapi config create-profile \
        --name myconnection_runtime \
        --nifi-url "https://<openflow-runtime-url>/nifi-api" \
        --auth-type bearer \
        --bearer-token "<your-pat-token>"

    # Verify connection
    nipyapi --profile myconnection_runtime ci get_status

  4.4 Deploy the PostgreSQL CDC Connector

    # List available connectors
    nipyapi --profile myconnection_runtime ci list_registry_flows \
        --registry_client ConnectorFlowRegistryClient \
        --bucket connectors

    # Deploy PostgreSQL connector
    nipyapi --profile myconnection_runtime ci deploy_flow \
        --registry_client ConnectorFlowRegistryClient \
        --bucket connectors \
        --flow postgresql

  Record the process_group_id from the output.

  4.5 Upload JDBC Driver (Required)

  The PostgreSQL JDBC driver is NOT bundled and must be uploaded:

    # Download the latest PostgreSQL JDBC driver
    curl -LO https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.7/postgresql-42.7.7.jar

    # Upload via nipyapi (find the parameter context first)
    nipyapi --profile myconnection_runtime parameters list_parameter_contexts

    # Upload the driver asset
    nipyapi --profile myconnection_runtime parameters upload_asset \
        --context_name "PostgreSQL CDC - Source" \
        --parameter_name "PostgreSQL JDBC Driver" \
        --file_path ./postgresql-42.7.7.jar

  4.6 Configure Source Parameters

    # Configure PostgreSQL source parameters
    nipyapi --profile myconnection_runtime ci configure_inherited_params \
        --process_group_id "<pg-id>" \
        --parameters '{
            "PostgreSQL Connection URL": "jdbc:postgresql://<privatelink-endpoint>:5432/<database>",
            "PostgreSQL Username": "openflow_replication",
            "PostgreSQL Password": "<your-password>",
            "Publication Name": "openflow_publication",
            "Included Table Names": "public.orders,public.customers"
        }'

  4.7 Configure Snowflake Destination Parameters

  For SPCS deployments, use SNOWFLAKE_SESSION_TOKEN authentication:

    nipyapi --profile myconnection_runtime ci configure_inherited_params \
        --process_group_id "<pg-id>" \
        --parameters '{
            "Snowflake Authentication Strategy": "SNOWFLAKE_SESSION_TOKEN",
            "Destination Database": "<YOUR_DESTINATION_DB>",
            "Snowflake Role": "<YOUR_ROLE>",
            "Snowflake Warehouse": "<YOUR_WAREHOUSE>"
        }'

  4.8 Configure Ingestion Parameters

    nipyapi --profile myconnection_runtime ci configure_inherited_params \
        --process_group_id "<pg-id>" \
        --parameters '{
            "Object Identifier Resolution": "CASE_SENSITIVE",
            "Ingestion Type": "incremental"
        }'

  Important: Object Identifier Resolution cannot be changed after initial replication. Choose:

  • CASE_SENSITIVE (default): Preserves lowercase names (requires quoted identifiers in SQL)
  • CASE_INSENSITIVE: Uppercases all names (standard Snowflake convention)

  4.9 Verify Configuration Before Starting

    # Verify controller services configuration
    nipyapi --profile myconnection_runtime ci verify_config \
        --process_group_id "<pg-id>" \
        --verify_processors=false

    # If verification passes, enable controller services
    nipyapi --profile myconnection_runtime ci enable_controllers \
        --process_group_id "<pg-id>"

    # Verify processors configuration
    nipyapi --profile myconnection_runtime ci verify_config \
        --process_group_id "<pg-id>" \
        --verify_controllers=false

  4.10 Start the CDC Flow

    # Start the flow
    nipyapi --profile myconnection_runtime ci start_flow \
        --process_group_id "<pg-id>"

    # Check status
    nipyapi --profile myconnection_runtime ci get_status \
        --process_group_id "<pg-id>"

  Expected output:

  • running_processors > 0
  • invalid_processors = 0
  • bulletin_errors = 0

  ────────────────────────────────────────

  Part 5: Validation & Testing

  5.1 Verify Initial Data Flow

    -- Check schema creation (quote lowercase names from PostgreSQL)
    SHOW SCHEMAS IN DATABASE <destination_database>;

    -- Check tables exist
    SHOW TABLES IN SCHEMA <destination_database>."public";

    -- Verify row counts
    SELECT COUNT(*) FROM <destination_database>."public"."orders";
    SELECT COUNT(*) FROM <destination_database>."public"."customers";

  5.2 Test CDC Changes

  On PostgreSQL source:

    -- Insert test record
    INSERT INTO public.orders (id, customer_id, amount, created_at)
    VALUES (99999, 1, 100.00, NOW());

    -- Update test record
    UPDATE public.orders SET amount = 150.00 WHERE id = 99999;

    -- Delete test record
    DELETE FROM public.orders WHERE id = 99999;

  On Snowflake destination (wait ~1 minute for batching):

    -- Verify changes replicated
    SELECT * FROM <destination_database>."public"."orders"
    WHERE id = 99999;

  5.3 Monitor CDC Health

    # Check for errors
    nipyapi --profile myconnection_runtime ci get_status \
        --process_group_id "<pg-id>"

    # Get detailed bulletins
    nipyapi --profile myconnection_runtime bulletins get_bulletin_board

  5.4 Check Table Replication State

    # Find TableStateService controller
    nipyapi --profile myconnection_runtime canvas list_all_controllers "<pg-id>" | \
        jq '.[] | select(.component.type | contains("TableState")) | {id: .id, name: .component.name}'

    # Get state entries
    nipyapi --profile myconnection_runtime canvas get_controller_state "<table-state-service-id>"

  Expected states:

  • SNAPSHOT_REPLICATION - Initial data load in progress
  • INCREMENTAL_REPLICATION - Streaming real-time changes
  • FAILED - Requires intervention

  ────────────────────────────────────────

  Part 6: Troubleshooting Common Issues

  6.1 Network Connectivity Issues

  ┌────────────────────────┬───────────────────────────────────────────────┬──────────────────────────────────────────────────────┐
  │ Error                  │ Cause                                         │ Resolution                                           │
  ├────────────────────────┼───────────────────────────────────────────────┼──────────────────────────────────────────────────────┤
  │ UnknownHostException   │ Host not in Network Rule or EAI not attached  │ Verify Network Rule, EAI, and attachment to Runtime  │
  ├────────────────────────┼───────────────────────────────────────────────┼──────────────────────────────────────────────────────┤
  │ SocketTimeoutException │ Port not in Network Rule or firewall blocking │ Check Network Rule includes :5432, verify NLB health │
  ├────────────────────────┼───────────────────────────────────────────────┼──────────────────────────────────────────────────────┤
  │ Connection refused     │ Service not listening                         │ Verify RDS is running and accepting connections      │
  └────────────────────────┴───────────────────────────────────────────────┴──────────────────────────────────────────────────────┘

  6.2 Authentication Issues

  ┌────────────────────────────────┬───────────────────────┬─────────────────────────────────────────────┐
  │ Error                          │ Cause                 │ Resolution                                  │
  ├────────────────────────────────┼───────────────────────┼─────────────────────────────────────────────┤
  │ password authentication failed │ Wrong credentials     │ Verify username/password in parameters      │
  ├────────────────────────────────┼───────────────────────┼─────────────────────────────────────────────┤
  │ no pg_hba.conf entry           │ Client IP not allowed │ Check RDS security group allows NLB subnets │
  ├────────────────────────────────┼───────────────────────┼─────────────────────────────────────────────┤
  │ FATAL: role does not exist     │ User not created      │ Create the replication user on PostgreSQL   │
  └────────────────────────────────┴───────────────────────┴─────────────────────────────────────────────┘

  6.3 Replication Issues

  ┌─────────────────────────────────┬───────────────────────────────────────┬───────────────────────────────────────────────┐
  │ Error                           │ Cause                                 │ Resolution                                    │
  ├─────────────────────────────────┼───────────────────────────────────────┼───────────────────────────────────────────────┤
  │ publication does not exist      │ Publication not created or wrong name │ Create publication, verify name in parameters │
  ├─────────────────────────────────┼───────────────────────────────────────┼───────────────────────────────────────────────┤
  │ table has no primary key        │ Missing PK                            │ Add primary key to source table               │
  ├─────────────────────────────────┼───────────────────────────────────────┼───────────────────────────────────────────────┤
  │ replication slot already exists │ Previous connector instance           │ Drop old slot or use different slot name      │
  └─────────────────────────────────┴───────────────────────────────────────┴───────────────────────────────────────────────┘

  6.4 Query Snowflake Event Logs

    -- Query OpenFlow runtime logs
    SELECT
        TIMESTAMP,
        RECORD_TYPE,
        RECORD['severity']::STRING AS severity,
        RECORD['message']::STRING AS message
    FROM <OPENFLOW_EVENTS_TABLE>
    WHERE TIMESTAMP > DATEADD('hour', -1, CURRENT_TIMESTAMP())
        AND RECORD['severity']::STRING IN ('ERROR', 'WARN')
    ORDER BY TIMESTAMP DESC
    LIMIT 50;

  ────────────────────────────────────────

  Security Best Practices

  1. Use dedicated replication user with minimal privileges (SELECT only on required tables)
  2. Private Link ensures traffic never traverses public internet
  3. Rotate credentials periodically and update OpenFlow parameters
  4. Monitor replication lag to detect issues early
  5. Use CASE_SENSITIVE naming to preserve source identifiers accurately
  6. Grant minimal Snowflake privileges to the runtime role

  ────────────────────────────────────────

  Quick Reference: Key Commands

    # Check flow status
    nipyapi --profile <profile> ci get_status --process_group_id "<pg-id>"

    # Stop flow
    nipyapi --profile <profile> ci stop_flow --process_group_id "<pg-id>"

    # Start flow
    nipyapi --profile <profile> ci start_flow --process_group_id "<pg-id>"

    # Check bulletins for errors
    nipyapi --profile <profile> bulletins get_bulletin_board

    # List deployed flows
    nipyapi --profile <profile> ci list_flows

  ────────────────────────────────────────
