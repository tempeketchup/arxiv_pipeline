locals {
  resource_prefix = "${var.project_name}-${var.environment}"

  # Storage account names must be globally unique, lowercase, no hyphens,
  # and <= 24 characters — different rules than most other Azure resources.
  storage_account_name = lower(replace("${var.project_name}${var.environment}sa", "-", ""))
}

resource "azurerm_resource_group" "this" {
  name     = "rg-${local.resource_prefix}"
  location = var.location
  tags     = var.tags
}

# ---------------------------------------------------------------------------
# Storage: ADLS Gen2 (hierarchical namespace enabled) — the bronze landing
# zone Airflow uploads to and Databricks reads from.
# ---------------------------------------------------------------------------
resource "azurerm_storage_account" "datalake" {
  name                     = local.storage_account_name
  resource_group_name      = azurerm_resource_group.this.name
  location                 = azurerm_resource_group.this.location
  account_tier             = "Standard"
  account_replication_type = "LRS" # upgrade to GRS/ZRS if cross-region durability matters
  is_hns_enabled           = true  # this flag is what makes it ADLS Gen2, not plain blob storage
  min_tls_version          = "TLS1_2"
  tags                     = var.tags
}

resource "azurerm_storage_data_lake_gen2_filesystem" "bronze" {
  name               = "bronze"
  storage_account_id = azurerm_storage_account.datalake.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "rejected" {
  name               = "rejected"
  storage_account_id = azurerm_storage_account.datalake.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "silver" {
  name               = "silver"
  storage_account_id = azurerm_storage_account.datalake.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "gold" {
  name               = "gold"
  storage_account_id = azurerm_storage_account.datalake.id
}


# ---------------------------------------------------------------------------
# Key Vault — a home for secrets the pipeline needs (e.g. a Databricks PAT
# for the Airflow connection, if you're not doing Azure AD auth end-to-end).
# Deliberately empty of actual secret values: those get added via `az
# keyvault secret set` or the portal, never committed as a tfvars value.
# ---------------------------------------------------------------------------
data "azurerm_client_config" "current" {}

resource "azurerm_key_vault" "this" {
  name                = "kv-${substr(local.resource_prefix, 0, 20)}"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name            = "standard"
  tags                = var.tags
}

resource "azurerm_key_vault_access_policy" "current_user" {
  key_vault_id = azurerm_key_vault.this.id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  object_id    = data.azurerm_client_config.current.object_id

  secret_permissions = ["Get", "List", "Set", "Delete"]
}

