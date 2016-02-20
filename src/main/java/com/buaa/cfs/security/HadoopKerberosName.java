/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.buaa.cfs.security;


import com.buaa.cfs.conf.Configuration;
import com.buaa.cfs.constant.CommonConfigurationKeys;
import com.buaa.cfs.utils.KerberosUtil;
import com.buaa.cfs.utils.SecurityUtil;

import java.io.IOException;


/**
 * This class implements parsing and handling of Kerberos principal names. In particular, it splits them apart and
 * translates them down into local operating system names.
 */
@SuppressWarnings("all")
public class HadoopKerberosName extends KerberosName {

    /**
     * Create a name from the full Kerberos principal name.
     *
     * @param name
     */
    public HadoopKerberosName(String name) {
        super(name);
    }

    /**
     * Set the static configuration to get the rules.
     * <p>
     * IMPORTANT: This method does a NOP if the rules have been set already. If there is a need to reset the rules, the
     * {@link KerberosName#setRules(String)} method should be invoked directly.
     *
     * @param conf the new configuration
     *
     * @throws IOException
     */
    public static void setConfiguration(Configuration conf) throws IOException {
        final String defaultRule;
        switch (SecurityUtil.getAuthenticationMethod(conf)) {
            case KERBEROS:
            case KERBEROS_SSL:
                try {
                    KerberosUtil.getDefaultRealm();
                } catch (Exception ke) {
                    throw new IllegalArgumentException("Can't get Kerberos realm", ke);
                }
                defaultRule = "DEFAULT";
                break;
            default:
                // just extract the simple user name
                defaultRule = "RULE:[1:$1] RULE:[2:$1]";
                break;
        }
        String ruleString = conf.get(CommonConfigurationKeys.HADOOP_SECURITY_AUTH_TO_LOCAL, defaultRule);
        setRules(ruleString);
    }

    public static void main(String[] args) throws Exception {
        setConfiguration(new Configuration());
        for (String arg : args) {
            HadoopKerberosName name = new HadoopKerberosName(arg);
            System.out.println("Name: " + name + " to " + name.getShortName());
        }
    }
}
