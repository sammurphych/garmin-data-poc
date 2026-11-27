terraform {
  required_version = ">= 1.6"
  required_providers {
    oci = {
      source  = "oracle/oci"
      version = "~> 7.0"
    }
  }
}

# Signs in with ~/.oci/config (an API key made under My profile → API keys in the Oracle console).
provider "oci" {}
