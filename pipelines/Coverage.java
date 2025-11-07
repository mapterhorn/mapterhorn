import java.nio.file.Path;
import java.io.IOException;
import java.nio.file.*;
import java.util.stream.Stream;
import java.util.List;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Planetiler;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.FeatureMerge;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.geo.GeometryException;


public class Coverage implements Profile {

  public static void main(String[] args) {
    var arguments = Arguments.fromArgs(args).withDefault("download", false);
    Path dir = Paths.get("polygon-store");
    var planetiler = Planetiler.create(arguments)
      .overwriteOutput(Path.of("bundle-store", "coverage.pmtiles"))
      .setProfile(new Coverage());

    try (Stream<Path> stream = Files.list(dir)) {
      for (Path path : stream.toList()) {
        if (path.toString().endsWith(".gpkg")) {
          String filename = path.getFileName().toString();
          String nameWithoutSuffix = filename.substring(0, filename.length() - 5);
          planetiler.addGeoPackageSource(nameWithoutSuffix, path, null);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    planetiler.run();
  }

  @Override
  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
    if (sourceFeature.canBePolygon()) {
      features.polygon("coverage")
        .setAttr("source", sourceFeature.getSource())
        .setPixelTolerance(6. * 0.0625);
    }
  }  
}