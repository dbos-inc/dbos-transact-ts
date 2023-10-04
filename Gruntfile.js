// eslint-disable-next-line @typescript-eslint/no-var-requires
var nbgv = require('nerdbank-gitversioning')

module.exports = function (grunt) {
    grunt.registerTask('setversion', function () {
        var done = this.async();
        nbgv.setPackageVersion().then(() => done());
    });
};