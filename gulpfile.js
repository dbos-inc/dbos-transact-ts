var gulp = require('gulp');
var nbgv = require('nerdbank-gitversioning')

gulp.task('setVersion', function() {
    return nbgv.setPackageVersion();
});

