module.exports = {
  apps : [
    {
      "name": "monitor:orders-queue",
      "script": "dist/src/checkOrder.js",
      "args": "",
      "instances": -1,
      "exec_mode": "fork"
    },
    {
      "name": "monitor:orders-visibility",
      "script": "dist/src/ordersVisibility.js",
      "args": "",
      "instances": 1,
      "exec_mode": "cluster",
      "comment": "This job is not required to be run. Its work is done in monitor:orders-queue. However it displays the order counts in the queues"
    }
  ],

  deploy : {
    production : {
      user : 'SSH_USERNAME',
      host : 'SSH_HOSTMACHINE',
      ref  : 'origin/master',
      repo : 'GIT_REPOSITORY',
      path : 'DESTINATION_PATH',
      'pre-deploy-local': '',
      'post-deploy' : 'npm install && pm2 reload ecosystem.config.js --env production',
      'pre-setup': ''
    }
  }
};
