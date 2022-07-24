/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.core.sql;

import org.apache.seatunnel.command.FlinkCommandArgs;
import org.apache.seatunnel.core.sql.job.Executor;
import org.apache.seatunnel.core.sql.job.JobInfo;
import org.apache.seatunnel.utils.CommandLineUtils;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SeatunnelSql {

    private static final Logger LOGGER = LoggerFactory.getLogger(SeatunnelSql.class);

    public static void main(String[] args) throws Exception {
        JobInfo jobInfo = parseJob(args);
        Executor.runJob(jobInfo);
    }

    private static JobInfo parseJob(String[] args) throws IOException {
        FlinkCommandArgs flinkArgs = CommandLineUtils.parseFlinkArgs(args);
        String configFilePath = flinkArgs.getConfigFile();
        String jobContent = FileUtils.readFileToString(new File(configFilePath), StandardCharsets.UTF_8);
        LOGGER.info("---------------origin sql--------------");
        LOGGER.info(jobContent);
        JobInfo jobInfo = new JobInfo(jobContent);
        /**
         * update by : david.dong
         * update time : 2022.06.09
         */
        Set<String> variableFields = parseVariableField(jobContent);
        List<String> variableValues = parseVariableVlaue(variableFields);
        jobInfo.substitute(variableValues);
        jobInfo.setJobContent(jobInfo.getJobContent().replace("%_%", " ").replace("@_@", "="));
        LOGGER.info("---------------run sql--------------");
        LOGGER.info(jobInfo.getJobContent());
        return jobInfo;
    }

    private static Set<String> parseVariableField(String content) {
        String pattern = "\\$\\{(.*?)\\}";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(content);
        Set<String> variables = new HashSet<String>();
        while (m.find()) {
            variables.add(m.group(1));
        }
        return variables;
    }

    private static List<String> parseVariableVlaue(Set<String> variables) {
        List<String> variableValue = new ArrayList<>();
        Set<String> systemVariables = System.getProperties().stringPropertyNames();
        for (String var : variables) {
            if (systemVariables.contains(var)) {
                variableValue.add(var + "=" + System.getProperty(var));
            }
        }
        return variableValue;
    }
}
