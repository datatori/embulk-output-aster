Embulk::JavaPlugin.register_output(
  "aster", "org.embulk.output.aster.AsterOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
