module "trigger_push_to_tag" {
  source              = "git::https://github.com/GlobalFishingWatch/gfw-terraform-modules.git//modules/cloudbuild-trigger?ref=v0.2.0"
  registry_artifact   = "devops"
  repo_name           = "gfw-ops"
  tag                 = ".*"
  trigger_description = "Builds and publishes a Docker image on every tag push." 
}

module "trigger_push_to_main" {
  source              = "git::https://github.com/GlobalFishingWatch/gfw-terraform-modules.git//modules/cloudbuild-trigger?ref=v0.2.0"
  registry_artifact   = "devops"
  repo_name           = "gfw-ops"
  branch              = "main"
  trigger_description = "Builds and publishes a Docker image on every push to the main branch."
}


