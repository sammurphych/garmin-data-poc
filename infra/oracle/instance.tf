# The Airflow server: an Always Free Ampere A1 VM (up to 4 OCPUs and 24 GB are free; this uses 1 and 6).
# airflow/server/install.sh sets up what runs on it.

data "oci_identity_availability_domains" "all" {
  compartment_id = local.compartment_id
}

# The newest Oracle Linux 9 image for the A1 shape, for a rebuild. The running server keeps the image it
# was made from: a newer image doesn't replace it (see ignore_changes).
data "oci_core_images" "oracle_linux_9" {
  compartment_id           = local.compartment_id
  operating_system         = "Oracle Linux"
  operating_system_version = "9"
  shape                    = "VM.Standard.A1.Flex"
  sort_by                  = "TIMECREATED"
  sort_order               = "DESC"
}

resource "oci_core_instance" "instance" {
  compartment_id = local.compartment_id
  # AD-1: the one with A1 capacity when this was made ("out of capacity" in the others).
  availability_domain = data.oci_identity_availability_domains.all.availability_domains[0].name
  display_name        = "instance-20261127-1247"
  shape               = "VM.Standard.A1.Flex"

  shape_config {
    ocpus         = 1
    memory_in_gbs = 6
  }

  source_details {
    source_type             = "image"
    source_id               = data.oci_core_images.oracle_linux_9.images[0].id
    boot_volume_size_in_gbs = "47"
  }

  create_vnic_details {
    subnet_id        = oci_core_subnet.public_subnet.id
    assign_public_ip = "true"
    hostname_label   = "instance-20261127-1247"
    display_name     = "instance-20261127-1247"
  }

  metadata = {
    ssh_authorized_keys = trimspace(file(pathexpand("~/.ssh/id_ed25519.pub")))
  }

  lifecycle {
    # Terraform refuses any plan that would delete the server.
    prevent_destroy = true
    ignore_changes = [
      defined_tags, freeform_tags, create_vnic_details[0].defined_tags,
      source_details[0].source_id, # a newer image must not replace the running server
    ]
  }
}

output "server_public_ip" {
  value = oci_core_instance.instance.public_ip
}
