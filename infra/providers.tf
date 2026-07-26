terraform {
  required_version = ">= 1.5"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.100"
    }
  }

  # Local state by default, which is fine for a solo portfolio project but
  # not for anything shared. To move to remote state, provision one storage
  # account out-of-band (outside this config, to avoid a chicken-and-egg
  # dependency) and uncomment:
  #
  # backend "azurerm" {
  #   resource_group_name  = "rg-tfstate"
  #   storage_account_name = "tfstatearxivetl"
  #   container_name       = "tfstate"
  #   key                  = "arxiv-pipeline.tfstate"
  # }
}

provider "azurerm" {
  features {}
}

