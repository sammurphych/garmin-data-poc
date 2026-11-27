# The network Oracle's "VCN with Internet Connectivity" wizard made: a public subnet for the server (internet
# gateway, SSH only from outside) and a private one (NAT and service gateways), unused for now.
#
# Oracle adds its own Oracle-Tags (who created what, and when); Terraform leaves them alone.

locals {
  compartment_id = var.tenancy_ocid # everything is in the root compartment
  vcn_cidr       = "10.0.0.0/16"
}

resource "oci_core_vcn" "airflow" {
  compartment_id = local.compartment_id
  display_name   = "airflow-vcn"
  cidr_blocks    = [local.vcn_cidr]
  dns_label      = "airflowvcn"

  lifecycle {
    prevent_destroy = true
    ignore_changes  = [defined_tags, freeform_tags]
  }
}

resource "oci_core_internet_gateway" "internet_gateway" {
  compartment_id = local.compartment_id
  vcn_id         = oci_core_vcn.airflow.id
  display_name   = "Internet gateway-airflow-vcn"
  enabled        = true

  lifecycle {
    ignore_changes = [defined_tags, freeform_tags]
  }
}

resource "oci_core_nat_gateway" "nat_gateway" {
  compartment_id = local.compartment_id
  vcn_id         = oci_core_vcn.airflow.id
  display_name   = "NAT gateway-airflow-vcn"

  lifecycle {
    ignore_changes = [defined_tags, freeform_tags]
  }
}

data "oci_core_services" "all" {
  filter {
    name   = "name"
    values = ["All .* Services In Oracle Services Network"]
    regex  = true
  }
}

resource "oci_core_service_gateway" "service_gateway" {
  compartment_id = local.compartment_id
  vcn_id         = oci_core_vcn.airflow.id
  display_name   = "Service gateway-airflow-vcn"

  services {
    service_id = data.oci_core_services.all.services[0].id
  }

  lifecycle {
    ignore_changes = [defined_tags, freeform_tags]
  }
}

# --- Public subnet: the server ---

resource "oci_core_default_route_table" "default_route_table" {
  manage_default_resource_id = oci_core_vcn.airflow.default_route_table_id
  display_name               = "default route table for airflow-vcn"

  route_rules {
    destination       = "0.0.0.0/0"
    destination_type  = "CIDR_BLOCK"
    network_entity_id = oci_core_internet_gateway.internet_gateway.id
  }

  lifecycle {
    ignore_changes = [defined_tags, freeform_tags]
  }
}

# The server's firewall. Only SSH comes in from the internet: Airflow's UI is reached through an SSH tunnel.
resource "oci_core_default_security_list" "default_security_list" {
  manage_default_resource_id = oci_core_vcn.airflow.default_security_list_id
  display_name               = "Default Security List for airflow-vcn"

  egress_security_rules {
    destination = "0.0.0.0/0"
    protocol    = "all"
  }

  ingress_security_rules {
    # SSH
    protocol = "6" # TCP
    source   = "0.0.0.0/0"
    tcp_options {
      min = 22
      max = 22
    }
  }

  ingress_security_rules {
    # ICMP fragmentation needed, for path MTU discovery
    protocol = "1"
    source   = "0.0.0.0/0"
    icmp_options {
      type = 3
      code = 4
    }
  }

  ingress_security_rules {
    # ICMP destination unreachable, from inside the VCN
    protocol = "1"
    source   = local.vcn_cidr
    icmp_options {
      type = 3
    }
  }

  lifecycle {
    ignore_changes = [defined_tags, freeform_tags]
  }
}

resource "oci_core_default_dhcp_options" "default_dhcp_options" {
  manage_default_resource_id = oci_core_vcn.airflow.default_dhcp_options_id
  display_name               = "Default DHCP Options for airflow-vcn"
  domain_name_type           = "CUSTOM_DOMAIN"

  options {
    type        = "DomainNameServer"
    server_type = "VcnLocalPlusInternet"
  }

  options {
    type                = "SearchDomain"
    search_domain_names = ["airflowvcn.oraclevcn.com"]
  }

  lifecycle {
    ignore_changes = [defined_tags, freeform_tags]
  }
}

resource "oci_core_subnet" "public_subnet" {
  compartment_id             = local.compartment_id
  vcn_id                     = oci_core_vcn.airflow.id
  display_name               = "public subnet-airflow-vcn"
  cidr_block                 = "10.0.0.0/24"
  dns_label                  = "sub11271246280" # the wizard's; changing it would recreate the subnet
  route_table_id             = oci_core_vcn.airflow.default_route_table_id
  security_list_ids          = [oci_core_vcn.airflow.default_security_list_id]
  dhcp_options_id            = oci_core_vcn.airflow.default_dhcp_options_id
  prohibit_internet_ingress  = false
  prohibit_public_ip_on_vnic = false

  lifecycle {
    ignore_changes = [defined_tags, freeform_tags]
  }
}

# --- Private subnet: unused, kept as the wizard made it ---

resource "oci_core_route_table" "private_route_table" {
  compartment_id = local.compartment_id
  vcn_id         = oci_core_vcn.airflow.id
  display_name   = "route table for private private subnet-airflow-vcn"

  route_rules {
    destination       = "0.0.0.0/0"
    destination_type  = "CIDR_BLOCK"
    network_entity_id = oci_core_nat_gateway.nat_gateway.id
  }

  route_rules {
    destination       = data.oci_core_services.all.services[0].cidr_block
    destination_type  = "SERVICE_CIDR_BLOCK"
    network_entity_id = oci_core_service_gateway.service_gateway.id
  }

  lifecycle {
    ignore_changes = [defined_tags, freeform_tags]
  }
}

resource "oci_core_security_list" "private_security_list" {
  compartment_id = local.compartment_id
  vcn_id         = oci_core_vcn.airflow.id
  display_name   = "security list for private subnet-airflow-vcn"

  egress_security_rules {
    destination = "0.0.0.0/0"
    protocol    = "all"
  }

  ingress_security_rules {
    # SSH from inside the VCN
    protocol = "6"
    source   = local.vcn_cidr
    tcp_options {
      min = 22
      max = 22
    }
  }

  ingress_security_rules {
    protocol = "1"
    source   = "0.0.0.0/0"
    icmp_options {
      type = 3
      code = 4
    }
  }

  ingress_security_rules {
    protocol = "1"
    source   = local.vcn_cidr
    icmp_options {
      type = 3
    }
  }

  lifecycle {
    ignore_changes = [defined_tags, freeform_tags]
  }
}

resource "oci_core_subnet" "private_subnet" {
  compartment_id             = local.compartment_id
  vcn_id                     = oci_core_vcn.airflow.id
  display_name               = "private subnet-airflow-vcn"
  cidr_block                 = "10.0.1.0/24"
  dns_label                  = "sub11271246281"
  route_table_id             = oci_core_route_table.private_route_table.id
  security_list_ids          = [oci_core_security_list.private_security_list.id]
  dhcp_options_id            = oci_core_vcn.airflow.default_dhcp_options_id
  prohibit_internet_ingress  = true
  prohibit_public_ip_on_vnic = true

  lifecycle {
    ignore_changes = [defined_tags, freeform_tags]
  }
}
