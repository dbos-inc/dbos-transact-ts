var gulp = require('gulp');
var nbgv = require('nerdbank-gitversioning')

gulp.task('setversion', function() {
    return nbgv.setPackageVersion();
});