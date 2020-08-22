NavinetProductInstaller_MavenArtifact "Spark SQL De-ID" do
  groupId node["research_DataImaging"]["groupId"]
  artifactId node["research_DataImaging"]["artifactId"]
  version node["research_DataImaging"]["version"]
  build node["research_DataImaging"]["build"]
  deployDependencies true

  destinationFolder node["research_DataImaging"]["destination"]
  consumerService node["research_DataImaging"]["service_name"]

  action :install
end

include_recipe("research_DataImaging::configure")