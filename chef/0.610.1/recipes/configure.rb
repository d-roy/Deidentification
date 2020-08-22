if !File.directory?("#{node["research_DataImaging"]["log_dir"]}")
  FileUtils.mkdir_p "#{node["research_DataImaging"]["log_dir"]}"
end