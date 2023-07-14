# Operon

Operon is a FaaS framework.

To build and test Operon, first clone this repository:
```shell
git clone https://github.com/dbos-inc/operon.git
cd operon/
```

Then, you may want to use [nvm](https://github.com/nvm-sh/nvm) to control node version. Operon currently builds and tests with node version 18.
```shell
nvm install 18
```

Now, you can install packages, build, and test operon:
```shell
npm install
npm run build
npm test
```