import * as gulp from "gulp";
import { setPackageVersion } from "nerdbank-gitversioning";

gulp.task("setversion", function () {
  return setPackageVersion();
});