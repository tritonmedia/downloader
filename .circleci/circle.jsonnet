local circle = import 'circle.libsonnet';

circle.ServiceConfig('downloader') {
  jobs+: {
    tests+: circle.Job(dockerImage='jaredallard/triton-base', withDocker=false) {
      steps_+:: [
        { restore_cache: { key: 'yarn-{{ checksum "package.json" }}' } },
        circle.RunStep('Fetch Dependencies', 'yarn --frozen-lockfile'),
        { save_cache: { key: 'yarn-{{ checksum "package.json" }}', paths: ['node_modules'] } },
        circle.RunStep('Run Tests', 'yarn test')
      ],
    },
  },
  workflows+: {
    ['build-push']+: {
      jobs_:: [
        'tests', 
        {
          name:: 'build',
          requires: ['tests'],
        }
      ],
    },
  },
}