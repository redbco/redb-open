# Enterprise Database Setup Guide

This guide provides detailed instructions for setting up native client libraries for ReDB's enterprise database adapters: IBM DB2, Oracle Database, and SAP HANA.

## Overview

ReDB supports enterprise databases through optional adapters that require native client libraries. These adapters are conditionally compiled using Go build tags to keep the community build lightweight and easy to compile.

**Enterprise Databases Supported:**
- IBM DB2
- Oracle Database  
- SAP HANA

**Build Modes:**
- **Community Build** (default): Excludes enterprise databases, no native dependencies required
- **Enterprise Build**: Includes all databases including DB2, Oracle, and HANA

## Prerequisites

- Go 1.21 or later
- CGO enabled (`CGO_ENABLED=1`)
- Native database client libraries (see platform-specific instructions below)
- C compiler (gcc, clang, or MSVC depending on platform)

## Building Enterprise Version

### Quick Start

```bash
# Build enterprise version for your host OS
make local-enterprise

# Build enterprise version for Linux (default)
make build-enterprise

# Build only the anchor service with enterprise support
make build-enterprise-anchor
```

The enterprise binaries will have an `-enterprise` suffix (e.g., `redb-anchor-enterprise`).

### Manual Build

```bash
# Set required environment variables
export CGO_ENABLED=1
export ENTERPRISE_BUILD=1

# Build with enterprise tag
go build -tags enterprise -o redb-anchor-enterprise ./services/anchor/cmd
```

## Platform-Specific Setup

### macOS

#### IBM DB2 on macOS

**1. Download IBM Data Server Driver Package**

- Visit [IBM Data Server Driver Package Downloads](https://www.ibm.com/support/pages/download-db2-fix-packs-version-db2-linux-unix-and-windows)
- Download "IBM Data Server Driver Package (DS Driver)" for macOS - e.g., `v12.1.0_macos_dsdriver.dmg`
- Or download from IBM Fix Central

**2. Install the Driver**

**Option A: Using DMG file (macOS recent versions)**

```bash
# Mount the DMG
hdiutil attach ~/Downloads/v12.1.0_macos_dsdriver.dmg

# Create installation directory
sudo mkdir -p /opt/ibm/db2

# Extract the tar.gz file from the mounted DMG to a temporary location
cd /tmp
tar -xzf /Volumes/dsdriver*/odbc_cli_driver/macos/ibm_data_server_driver_for_odbc_cli.tar.gz

# Move the extracted clidriver to the installation directory
sudo mv clidriver /opt/ibm/db2/

# Unmount the DMG (cleanup)
hdiutil detach /Volumes/dsdriver*

# Verify installation
ls -la /opt/ibm/db2/clidriver/lib/
ls -la /opt/ibm/db2/clidriver/include/

# Set up environment variables
export IBM_DB_HOME=/opt/ibm/db2/clidriver
export DYLD_LIBRARY_PATH=$IBM_DB_HOME/lib:$DYLD_LIBRARY_PATH
export CGO_CFLAGS="-I$IBM_DB_HOME/include"
export CGO_LDFLAGS="-L$IBM_DB_HOME/lib -ldb2"

# Add to ~/.zshrc or ~/.bash_profile for persistence
cat >> ~/.zshrc << 'EOF'

# IBM DB2 Configuration
export IBM_DB_HOME=/opt/ibm/db2/clidriver
export DYLD_LIBRARY_PATH=$IBM_DB_HOME/lib:$DYLD_LIBRARY_PATH
export CGO_CFLAGS="-I$IBM_DB_HOME/include"
export CGO_LDFLAGS="-L$IBM_DB_HOME/lib -ldb2"
EOF

# Reload shell configuration
source ~/.zshrc
```

**Option B: Using TAR.GZ archive (older distributions)**

```bash
# Extract the downloaded archive
tar -xzf ibm_data_server_driver_package_darwin*.tar.gz

# Move to a permanent location
sudo mkdir -p /opt/ibm
sudo mv dsdriver /opt/ibm/db2

# Set up environment variables (same as Option A)
export IBM_DB_HOME=/opt/ibm/db2
export DYLD_LIBRARY_PATH=$IBM_DB_HOME/lib:$DYLD_LIBRARY_PATH
export CGO_CFLAGS="-I$IBM_DB_HOME/include"
export CGO_LDFLAGS="-L$IBM_DB_HOME/lib -ldb2"

# Add to ~/.zshrc
echo 'export IBM_DB_HOME=/opt/ibm/db2' >> ~/.zshrc
echo 'export DYLD_LIBRARY_PATH=$IBM_DB_HOME/lib:$DYLD_LIBRARY_PATH' >> ~/.zshrc
echo 'export CGO_CFLAGS="-I$IBM_DB_HOME/include"' >> ~/.zshrc
echo 'export CGO_LDFLAGS="-L$IBM_DB_HOME/lib -ldb2"' >> ~/.zshrc

# Reload shell configuration
source ~/.zshrc
```

**3. Verify Installation**

```bash
# Check if library files exist
ls -la $IBM_DB_HOME/lib/libdb2*

# Test with db2level command (if CLI tools are included)
$IBM_DB_HOME/bin/db2level
```

**4. Configure DSN (Optional)**

Create a db2dsdriver.cfg file for connection configuration:

```xml
<?xml version="1.0" encoding="UTF-8" ?>
<configuration>
  <dsncollection>
    <dsn alias="MYDB2" name="SAMPLE" host="localhost" port="50000"/>
  </dsncollection>
  <databases>
    <database name="SAMPLE" host="localhost" port="50000">
      <parameter name="Authentication" value="SERVER"/>
    </database>
  </databases>
</configuration>
```

#### Oracle Database on macOS

**1. Download Oracle Instant Client**

- Visit [Oracle Instant Client Downloads](https://www.oracle.com/database/technologies/instant-client/downloads.html)
- Download for macOS (Intel or Apple Silicon):
  - **Basic Package** (required) - e.g., `instantclient-basic-macos.arm64-23.3.0.23.09-2.dmg`
  - **SDK Package** (required for development) - e.g., `instantclient-sdk-macos.arm64-23.3.0.23.09.dmg`
  - **SQL*Plus Package** (optional, for testing)

**2. Install the Client**

**Option A: Using DMG files (macOS 23.x and later)**

```bash
# Mount the Basic DMG
hdiutil attach ~/Downloads/instantclient-basic-macos.*.dmg

# Mount the SDK DMG
hdiutil attach ~/Downloads/instantclient-sdk-macos.*.dmg

# Create installation directory
sudo mkdir -p /opt/oracle/instantclient

# Copy contents from mounted Basic volume (files are at root of volume)
sudo cp -R /Volumes/instantclient-basic-macos*/* /opt/oracle/instantclient/

# Copy SDK contents from mounted SDK volume
sudo cp -R /Volumes/instantclient-sdk-macos*/sdk /opt/oracle/instantclient/

# Unmount the DMG files
hdiutil detach /Volumes/instantclient-basic-macos*
hdiutil detach /Volumes/instantclient-sdk-macos*

# Create symbolic links (adjust version number to match your install)
cd /opt/oracle/instantclient
sudo ln -sf libclntsh.dylib.23.1 libclntsh.dylib
sudo ln -sf libocci.dylib.23.1 libocci.dylib

# Set up environment variables
export ORACLE_HOME=/opt/oracle/instantclient
export DYLD_LIBRARY_PATH=$ORACLE_HOME:$DYLD_LIBRARY_PATH
export CGO_CFLAGS="-I$ORACLE_HOME/sdk/include"
export CGO_LDFLAGS="-L$ORACLE_HOME -lclntsh"
export PATH=$ORACLE_HOME:$PATH

# Add to ~/.zshrc or ~/.bash_profile for persistence
echo 'export ORACLE_HOME=/opt/oracle/instantclient' >> ~/.zshrc
echo 'export DYLD_LIBRARY_PATH=$ORACLE_HOME:$DYLD_LIBRARY_PATH' >> ~/.zshrc
echo 'export CGO_CFLAGS="-I$ORACLE_HOME/sdk/include"' >> ~/.zshrc
echo 'export CGO_LDFLAGS="-L$ORACLE_HOME -lclntsh"' >> ~/.zshrc
echo 'export PATH=$ORACLE_HOME:$PATH' >> ~/.zshrc

# Reload shell configuration
source ~/.zshrc
```

**Option B: Using ZIP files (older versions)**

```bash
# Create installation directory
sudo mkdir -p /opt/oracle

# Extract downloaded zip files
unzip instantclient-basic-macos.x64-*.zip -d /opt/oracle
unzip instantclient-sdk-macos.x64-*.zip -d /opt/oracle

# Rename to standard path
cd /opt/oracle
sudo mv instantclient_* instantclient

# Create symbolic links
cd /opt/oracle/instantclient
sudo ln -sf libclntsh.dylib.* libclntsh.dylib
sudo ln -sf libocci.dylib.* libocci.dylib

# Set up environment variables (same as Option A above)
export ORACLE_HOME=/opt/oracle/instantclient
export DYLD_LIBRARY_PATH=$ORACLE_HOME:$DYLD_LIBRARY_PATH
export CGO_CFLAGS="-I$ORACLE_HOME/sdk/include"
export CGO_LDFLAGS="-L$ORACLE_HOME -lclntsh"
export PATH=$ORACLE_HOME:$PATH

# Add to ~/.zshrc
echo 'export ORACLE_HOME=/opt/oracle/instantclient' >> ~/.zshrc
echo 'export DYLD_LIBRARY_PATH=$ORACLE_HOME:$DYLD_LIBRARY_PATH' >> ~/.zshrc
echo 'export CGO_CFLAGS="-I$ORACLE_HOME/sdk/include"' >> ~/.zshrc
echo 'export CGO_LDFLAGS="-L$ORACLE_HOME -lclntsh"' >> ~/.zshrc
echo 'export PATH=$ORACLE_HOME:$PATH' >> ~/.zshrc

# Reload shell configuration
source ~/.zshrc
```

**3. Verify Installation**

```bash
# Check if library files exist
ls -la $ORACLE_HOME/libclntsh*

# Test with SQL*Plus (if installed)
sqlplus -v
```

**4. Configure TNS (Optional)**

Create a tnsnames.ora file in `$ORACLE_HOME/network/admin/`:

```
MYDB =
  (DESCRIPTION =
    (ADDRESS = (PROTOCOL = TCP)(HOST = localhost)(PORT = 1521))
    (CONNECT_DATA =
      (SERVER = DEDICATED)
      (SERVICE_NAME = ORCL)
    )
  )
```

#### SAP HANA on macOS

**1. Download SAP HANA Client**

- Visit [SAP Development Tools](https://tools.hana.ondemand.com/)
- Download "SAP HANA Client" for macOS
- Or use SAP Software Download Center (requires SAP credentials)

**2. Install the Client**

Using the installer:

```bash
# Extract the downloaded archive
tar -xzf hanaclient-*.tar.gz
cd hanaclient

# Run the installer
sudo ./hdbinst

# Follow the interactive prompts:
# - Installation Path: /usr/sap/hdbclient (recommended)
# - Components: Select all
```

Using manual extraction:

```bash
# Extract to a permanent location
sudo mkdir -p /usr/sap/hdbclient
sudo tar -xzf HDBCLIENT*.TGZ -C /usr/sap/hdbclient

# Set up environment variables
export HANA_HOME=/usr/sap/hdbclient
export DYLD_LIBRARY_PATH=$HANA_HOME:$DYLD_LIBRARY_PATH
export CGO_CFLAGS="-I$HANA_HOME"
export CGO_LDFLAGS="-L$HANA_HOME -ldbcapiHDB"
export PATH=$HANA_HOME:$PATH

# Add to ~/.zshrc or ~/.bash_profile for persistence
echo 'export HANA_HOME=/usr/sap/hdbclient' >> ~/.zshrc
echo 'export DYLD_LIBRARY_PATH=$HANA_HOME:$DYLD_LIBRARY_PATH' >> ~/.zshrc
echo 'export CGO_CFLAGS="-I$HANA_HOME"' >> ~/.zshrc
echo 'export CGO_LDFLAGS="-L$HANA_HOME -ldbcapiHDB"' >> ~/.zshrc
echo 'export PATH=$HANA_HOME:$PATH' >> ~/.zshrc
```

**3. Verify Installation**

```bash
# Check if library files exist
ls -la $HANA_HOME/libdbcapiHDB*

# Test with hdbsql
hdbsql -v
```

**4. Configure hdbuserstore (Optional)**

Store connection details securely:

```bash
# Add a connection
hdbuserstore SET MYKEY hostname:30015 USERNAME PASSWORD

# List stored connections
hdbuserstore LIST

# Test connection
hdbsql -U MYKEY "SELECT * FROM DUMMY"
```

### Linux

#### IBM DB2 on Linux

```bash
# Download DB2 Driver
wget https://public.dhe.ibm.com/ibmdl/export/pub/software/data/db2/drivers/odbc_cli/linuxx64_odbc_cli.tar.gz

# Extract and install
tar -xzf linuxx64_odbc_cli.tar.gz
sudo mv clidriver /opt/ibm/db2

# Set environment variables
export IBM_DB_HOME=/opt/ibm/db2
export LD_LIBRARY_PATH=$IBM_DB_HOME/lib:$LD_LIBRARY_PATH
export CGO_CFLAGS="-I$IBM_DB_HOME/include"
export CGO_LDFLAGS="-L$IBM_DB_HOME/lib -ldb2"

# Add to ~/.bashrc
echo 'export IBM_DB_HOME=/opt/ibm/db2' >> ~/.bashrc
echo 'export LD_LIBRARY_PATH=$IBM_DB_HOME/lib:$LD_LIBRARY_PATH' >> ~/.bashrc
echo 'export CGO_CFLAGS="-I$IBM_DB_HOME/include"' >> ~/.bashrc
echo 'export CGO_LDFLAGS="-L$IBM_DB_HOME/lib -ldb2"' >> ~/.bashrc
```

#### Oracle Database on Linux

```bash
# Download Oracle Instant Client (Basic + SDK)
wget https://download.oracle.com/otn_software/linux/instantclient/[VERSION]/instantclient-basic-linux.x64-[VERSION].zip
wget https://download.oracle.com/otn_software/linux/instantclient/[VERSION]/instantclient-sdk-linux.x64-[VERSION].zip

# Extract
sudo mkdir -p /opt/oracle
sudo unzip instantclient-basic-linux.x64-*.zip -d /opt/oracle
sudo unzip instantclient-sdk-linux.x64-*.zip -d /opt/oracle

# Set environment variables
export ORACLE_HOME=/opt/oracle/instantclient_[VERSION]
export LD_LIBRARY_PATH=$ORACLE_HOME:$LD_LIBRARY_PATH
export CGO_CFLAGS="-I$ORACLE_HOME/sdk/include"
export CGO_LDFLAGS="-L$ORACLE_HOME -lclntsh"

# Add to ~/.bashrc
echo 'export ORACLE_HOME=/opt/oracle/instantclient_[VERSION]' >> ~/.bashrc
echo 'export LD_LIBRARY_PATH=$ORACLE_HOME:$LD_LIBRARY_PATH' >> ~/.bashrc
echo 'export CGO_CFLAGS="-I$ORACLE_HOME/sdk/include"' >> ~/.bashrc
echo 'export CGO_LDFLAGS="-L$ORACLE_HOME -lclntsh"' >> ~/.bashrc

# Configure dynamic linker
echo $ORACLE_HOME | sudo tee /etc/ld.so.conf.d/oracle.conf
sudo ldconfig
```

#### SAP HANA on Linux

```bash
# Download SAP HANA Client
# (Requires SAP credentials or available from your HANA system)

# Extract and install
tar -xzf hanaclient-*.tar.gz
cd hanaclient
sudo ./hdbinst --path=/usr/sap/hdbclient

# Set environment variables
export HANA_HOME=/usr/sap/hdbclient
export LD_LIBRARY_PATH=$HANA_HOME:$LD_LIBRARY_PATH
export CGO_CFLAGS="-I$HANA_HOME"
export CGO_LDFLAGS="-L$HANA_HOME -ldbcapiHDB"

# Add to ~/.bashrc
echo 'export HANA_HOME=/usr/sap/hdbclient' >> ~/.bashrc
echo 'export LD_LIBRARY_PATH=$HANA_HOME:$LD_LIBRARY_PATH' >> ~/.bashrc
echo 'export CGO_CFLAGS="-I$HANA_HOME"' >> ~/.bashrc
echo 'export CGO_LDFLAGS="-L$HANA_HOME -ldbcapiHDB"' >> ~/.bashrc
```

### Windows

#### IBM DB2 on Windows

1. Download "IBM Data Server Driver Package" for Windows
2. Run the installer executable
3. Follow the installation wizard
4. Set environment variables in System Properties:
   - `IBM_DB_HOME`: C:\Program Files\IBM\SQLLIB
   - Add `%IBM_DB_HOME%\bin` to `PATH`
   - `CGO_CFLAGS`: -IC:\Program Files\IBM\SQLLIB\include
   - `CGO_LDFLAGS`: -LC:\Program Files\IBM\SQLLIB\lib -ldb2api

#### Oracle Database on Windows

1. Download Oracle Instant Client for Windows
2. Extract to `C:\oracle\instantclient_[VERSION]`
3. Set environment variables:
   - `ORACLE_HOME`: C:\oracle\instantclient_[VERSION]
   - Add `%ORACLE_HOME%` to `PATH`
   - `CGO_CFLAGS`: -IC:\oracle\instantclient_[VERSION]\sdk\include
   - `CGO_LDFLAGS`: -LC:\oracle\instantclient_[VERSION] -loci

#### SAP HANA on Windows

1. Download SAP HANA Client installer for Windows
2. Run the MSI installer
3. Follow the installation wizard
4. Set environment variables:
   - `HANA_HOME`: C:\Program Files\SAP\hdbclient
   - Add `%HANA_HOME%` to `PATH`
   - `CGO_CFLAGS`: -IC:\Program Files\SAP\hdbclient
   - `CGO_LDFLAGS`: -LC:\Program Files\SAP\hdbclient -ldbcapiHDB

## CDC (Change Data Capture) Setup

### IBM DB2 CDC

DB2 supports multiple CDC mechanisms:

#### Option 1: SQL Replication (Recommended for Production)

Requires DB2 Replication Server setup (separate product).

#### Option 2: Trigger-Based CDC (Built-in)

```sql
-- Enable archiving (required for some CDC methods)
UPDATE DATABASE CONFIGURATION FOR SAMPLE USING LOGARCHMETH1 DISK:/db2/archive;

-- Create CDC tracking table
CREATE TABLE CDC_TRACKING (
    TABLE_NAME VARCHAR(128),
    OPERATION VARCHAR(10),
    CHANGE_TIME TIMESTAMP,
    CHANGE_DATA XML
);

-- Create triggers on source table
CREATE TRIGGER MYTABLE_INSERT_CDC
AFTER INSERT ON MYTABLE
REFERENCING NEW AS N
FOR EACH ROW
BEGIN ATOMIC
    INSERT INTO CDC_TRACKING VALUES (
        'MYTABLE',
        'INSERT',
        CURRENT_TIMESTAMP,
        XMLELEMENT(NAME "row", XMLATTRIBUTES(N.* AS "*"))
    );
END;
```

#### Option 3: Q Replication

Setup Q Replication queues for asynchronous replication.

### Oracle CDC with LogMiner

LogMiner is Oracle's built-in CDC mechanism.

#### Enable Supplemental Logging

```sql
-- Enable minimal supplemental logging at database level
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;

-- Enable primary key logging
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS;

-- Enable all columns logging (for specific table)
ALTER TABLE my_schema.my_table ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

-- Verify supplemental logging is enabled
SELECT SUPPLEMENTAL_LOG_DATA_MIN, SUPPLEMENTAL_LOG_DATA_PK
FROM V$DATABASE;
```

#### Enable Archive Log Mode (Required)

```sql
-- Check current log mode
SELECT LOG_MODE FROM V$DATABASE;

-- If NOARCHIVELOG, enable it:
SHUTDOWN IMMEDIATE;
STARTUP MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;
```

#### Using LogMiner

```sql
-- Start LogMiner session
BEGIN
    DBMS_LOGMNR.START_LOGMNR(
        STARTSCN => 1000000,
        OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + 
                   DBMS_LOGMNR.CONTINUOUS_MINE
    );
END;
/

-- Query changes
SELECT OPERATION, SQL_REDO, SQL_UNDO, TIMESTAMP
FROM V$LOGMNR_CONTENTS
WHERE SEG_OWNER = 'MY_SCHEMA'
AND TABLE_NAME = 'MY_TABLE'
AND OPERATION IN ('INSERT', 'UPDATE', 'DELETE');

-- End LogMiner session
BEGIN
    DBMS_LOGMNR.END_LOGMNR;
END;
/
```

### SAP HANA CDC

HANA supports multiple CDC approaches:

#### Option 1: Trigger-Based CDC (Recommended for ReDB)

```sql
-- Create CDC log table
CREATE COLUMN TABLE MY_TABLE_CDC_LOG (
    -- Copy all columns from source table
    COL1 VARCHAR(100),
    COL2 INTEGER,
    -- Add CDC metadata columns
    CDC_OPERATION VARCHAR(10),
    CDC_TIMESTAMP TIMESTAMP,
    CDC_COMMIT_ID BIGINT GENERATED ALWAYS AS IDENTITY
);

-- Create INSERT trigger
CREATE TRIGGER MY_TABLE_CDC_INSERT_TRG
AFTER INSERT ON MY_TABLE
REFERENCING NEW ROW AS NEW_ROW
FOR EACH ROW
BEGIN
    INSERT INTO MY_TABLE_CDC_LOG 
    VALUES (:NEW_ROW.COL1, :NEW_ROW.COL2, 'INSERT', CURRENT_TIMESTAMP, DEFAULT);
END;

-- Create UPDATE trigger
CREATE TRIGGER MY_TABLE_CDC_UPDATE_TRG
AFTER UPDATE ON MY_TABLE
REFERENCING NEW ROW AS NEW_ROW
FOR EACH ROW
BEGIN
    INSERT INTO MY_TABLE_CDC_LOG 
    VALUES (:NEW_ROW.COL1, :NEW_ROW.COL2, 'UPDATE', CURRENT_TIMESTAMP, DEFAULT);
END;

-- Create DELETE trigger
CREATE TRIGGER MY_TABLE_CDC_DELETE_TRG
AFTER DELETE ON MY_TABLE
REFERENCING OLD ROW AS OLD_ROW
FOR EACH ROW
BEGIN
    INSERT INTO MY_TABLE_CDC_LOG 
    VALUES (:OLD_ROW.COL1, :OLD_ROW.COL2, 'DELETE', CURRENT_TIMESTAMP, DEFAULT);
END;
```

#### Option 2: Smart Data Integration (SDI)

Requires SAP HANA Smart Data Integration setup (additional license).

#### Option 3: Application-Level CDC

Use HANA system views to track changes:
- `SYS.M_TRANSACTIONAL_LOCKS`
- `SYS.M_TRANSACTIONS`

## Troubleshooting

### Common Issues

#### "cgo: C compiler not found"

**Solution:** Install a C compiler
- macOS: `xcode-select --install`
- Linux: `sudo apt-get install build-essential` or `sudo yum install gcc`
- Windows: Install MinGW-w64 or Visual Studio Build Tools

#### "library not found" or "cannot find -ldb2"

**Solution:** Verify library paths
```bash
# macOS/Linux
echo $DYLD_LIBRARY_PATH  # macOS
echo $LD_LIBRARY_PATH    # Linux

# Check if library exists
ls -la $IBM_DB_HOME/lib/libdb2*
ls -la $ORACLE_HOME/libclntsh*
ls -la $HANA_HOME/libdbcapiHDB*
```

#### "undefined symbol" errors

**Solution:** Ensure CGO flags are set correctly and libraries are compatible with your system architecture (x86_64 vs ARM64).

#### Enterprise build fails with "package not found"

**Solution:** Ensure you're building with the enterprise tag:
```bash
go build -tags enterprise ./...
```

### Testing Your Setup

Create a simple test program:

```go
package main

import (
    "database/sql"
    "fmt"
    _ "github.com/ibmdb/go_ibm_db"  // DB2
    _ "github.com/godror/godror"     // Oracle
    _ "github.com/SAP/go-hdb/driver" // HANA
)

func main() {
    // Test DB2
    db, err := sql.Open("go_ibm_db", "HOSTNAME=localhost;DATABASE=sample;UID=user;PWD=pass")
    if err != nil {
        fmt.Printf("DB2 error: %v\n", err)
    } else {
        defer db.Close()
        fmt.Println("DB2 connection successful!")
    }

    // Test Oracle
    db, err = sql.Open("godror", "user/pass@localhost:1521/ORCL")
    if err != nil {
        fmt.Printf("Oracle error: %v\n", err)
    } else {
        defer db.Close()
        fmt.Println("Oracle connection successful!")
    }

    // Test HANA
    db, err = sql.Open("hdb", "hdb://user:pass@localhost:30015")
    if err != nil {
        fmt.Printf("HANA error: %v\n", err)
    } else {
        defer db.Close()
        fmt.Println("HANA connection successful!")
    }
}
```

Build and run:
```bash
CGO_ENABLED=1 go run test.go
```

## Security Considerations

1. **Never commit credentials** to version control
2. **Use environment variables** or secure vaults for passwords
3. **Enable SSL/TLS** for database connections in production
4. **Restrict database user permissions** to minimum required
5. **Regularly update** client libraries for security patches
6. **Use hdbuserstore** (HANA) or Oracle Wallet for credential management

## Performance Tips

1. **Connection Pooling**: Configure appropriate pool sizes
2. **Prepared Statements**: Use for repeated queries
3. **Batch Operations**: Process multiple rows in single transactions
4. **CDC Tuning**: Adjust log retention and polling intervals
5. **Indexing**: Ensure CDC log tables are properly indexed

## Support and Resources

### Official Documentation
- [IBM DB2 Documentation](https://www.ibm.com/docs/en/db2)
- [Oracle Database Documentation](https://docs.oracle.com/en/database/)
- [SAP HANA Documentation](https://help.sap.com/hana)

### Driver Documentation
- [go_ibm_db](https://github.com/ibmdb/go_ibm_db)
- [godror](https://github.com/godror/godror)
- [go-hdb](https://github.com/SAP/go-hdb)

### ReDB Resources
- GitHub: https://github.com/redbco/redb-open
- Documentation: See `docs/` directory
- Issues: GitHub Issues page

## License Notes

- **ReDB**: Dual-licensed (see LICENSE and LICENSE-COMMERCIAL.md)
- **Native Client Libraries**: Subject to vendor licenses
  - IBM DB2: IBM license agreement
  - Oracle: Oracle Technology Network License
  - SAP HANA: SAP Developer License or commercial license

Always review and comply with vendor license terms when using enterprise database clients.

