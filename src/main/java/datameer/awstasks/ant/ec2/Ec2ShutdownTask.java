/**
 * Copyright 2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package datameer.awstasks.ant.ec2;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import awstasks.com.amazonaws.services.ec2.AmazonEC2;
import awstasks.com.amazonaws.services.ec2.model.DeleteSecurityGroupRequest;
import awstasks.com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import awstasks.com.amazonaws.services.ec2.model.DescribeInstancesResult;
import awstasks.com.amazonaws.services.ec2.model.Instance;
import awstasks.com.amazonaws.services.ec2.model.InstanceStateName;
import datameer.awstasks.aws.ec2.InstanceGroup;
import datameer.awstasks.util.Ec2Util;

public class Ec2ShutdownTask extends AbstractEc2ConnectTask {

    private boolean _stopOnly = false;

    public boolean isStopOnly() {
        return _stopOnly;
    }

    public void setStopOnly(boolean stopOnly) {
        _stopOnly = stopOnly;
    }

    @Override
    protected void doExecute(AmazonEC2 ec2, InstanceGroup instanceGroup) throws Exception {
        LOG.info("executing " + getClass().getSimpleName() + " with groupName '" + _groupName + "'");
        if (_stopOnly) {
            instanceGroup.stop();
        } else {
            Set<String> instanceIds = new HashSet<String>();
            for (Instance instance : instanceGroup.getInstances(false)) {
                instanceIds.add(instance.getInstanceId());
            }
            instanceGroup.terminate();
            if (Ec2Util.groupExists(ec2, _groupName)) {
                LOG.info("group '" + _groupName + "' exists - deleting it.");
                DescribeInstancesResult describeInstancesResult = ec2
                        .describeInstances(new DescribeInstancesRequest().withInstanceIds(instanceIds));
                Ec2Util.waitUntil(ec2, describeInstancesResult.getReservations().get(0).getInstances(), EnumSet.allOf(InstanceStateName.class), InstanceStateName.Terminated, TimeUnit.MINUTES, 2);
                LOG.info("Instances terminated. Deleting security group: '" + _groupName + "'.");
                ec2.deleteSecurityGroup(new DeleteSecurityGroupRequest(_groupName));
            }
        }
    }
}
