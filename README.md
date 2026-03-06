# Secure Stock Management SaaS

A comprehensive, multi-tenant inventory counting and issue reporting system built entirely on Streamlit and SQLite. This application allows multiple organizations (Workspaces) to manage inventory stock counts, report damaged/unlisted items, track employee activity, and export clean data formats.

## Features

- **Multi-Tenant Architecture:** Isolated Workspaces for different clients or branches.
- **Role-Based Access Control (RBAC):** Customizable roles allowing granular permission management per user (e.g., restricted locations, specific feature tabs).
- **Interactive Counting Portal:** - Supports image attachments for proof.
  - "Quick Entry" toggle for rapid, barcode-scanner-like stock punching.
  - Retains history of all counts by user for auditability.
- **Smart Imports:** Upload bulk locations and multi-location inventory directly via CSV/Excel. Missing locations are automatically created.
- **Issue Tracking:** Report discrepancies or damaged items tied to specific database objects or unlisted items.
- **Dynamic Exports:** Download Master Inventory logic and deep-dive logs via Excel, alongside bulk Image archives. Build and save custom export formatting presets.

## Installation & Setup

1. **Prerequisites:** Ensure you have Python 3.8+ installed on your system.
2. **Clone/Download** this repository.
3. **Install Dependencies:** Open your terminal or command prompt in the folder containing `app.py` and run:
   ```bash
   pip install -r requirements.txt
   Usage Guide
1. Initial Setup
Open the app in your browser (usually http://localhost:8501).

Navigate to the "Sign Up" tab and create your first account.

Once logged in, go to the "👤 My Profile" tab and create a new Workspace. You will automatically be made the Super Admin of this space.

2. Administrator Settings
Go to "⚙️ Masters & Settings".

Locations: Create physical locations (or upload an Excel file of location names).

Users & Invites: Type in the username of another person who has signed up to invite them to your workspace.

Settings: Control inactive timeouts, stock decimal places, turn on Quick Entry mode, and enforce mandatory photo/comment validations for issue reports.

3. Importing Stock Data
Go to "📥 Location Import".

Choose between a single-location replace, or use the "Multi-Location Import" toggle to upload a master sheet containing a "Location" column to populate the entire database at once.

4. Counting Operations
Navigate to the "📝 Counting Portal".

Select a location.

Type a quantity into the quick entry box or expand the item row to add notes/images.

Type a number and press Enter to register the number. Click "Submit Count" to lock it into the database.

5. Exporting
Navigate to "📁 Data Export & Reports".

Rearrange columns to fit your specific needs and save it as a "Preset".

Generate a complete Excel backup consisting of the master inventory comparison, deep count logs, and issue reports.

Architecture Notes
Database: Utilizes a thread-safe sqlite3 setup. The database file (stock_pro_saas_v3.db) is automatically created and migrated on the first run. No external SQL server is required.

Session Handling: Includes concurrent login prevention. If a user logs in on a new device, their previous session token is wiped, ensuring strict data security.
