output "resource_group_name" {
  value = azurerm_resource_group.this.name
}

output "storage_account_name" {
  value = azurerm_storage_account.datalake.name
}

output "storage_account_primary_access_key" {
  value     = azurerm_storage_account.datalake.primary_access_key
  sensitive = true
}

output "key_vault_uri" {
  value = azurerm_key_vault.this.vault_uri
}
