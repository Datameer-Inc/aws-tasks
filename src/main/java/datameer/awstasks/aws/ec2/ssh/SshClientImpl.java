/**
 * Copyright 2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package datameer.awstasks.aws.ec2.ssh;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import datameer.awstasks.ssh.JschRunner;
import datameer.awstasks.ssh.ScpDownloadCommand;
import datameer.awstasks.ssh.ScpUploadCommand;
import datameer.awstasks.ssh.SshExecCommand;
import datameer.awstasks.util.ExceptionUtil;
import datameer.awstasks.util.IoUtil;
import datameer.com.google.common.base.Throwables;
import datameer.com.google.common.collect.Lists;

public class SshClientImpl implements SshClient {

    protected static final Logger LOG = Logger.getLogger(SshClientImpl.class);
    protected File _privateKey;
    protected String _password;
    protected final String _username;
    protected final List<String> _hostnames;
    private boolean _enableConnectRetries;

    public SshClientImpl(String username, File privateKey, List<String> hostnames) {
        _username = username;
        _privateKey = privateKey;
        _hostnames = hostnames;
    }

    public SshClientImpl(String username, String password, List<String> hostnames) {
        _username = username;
        _password = password;
        _hostnames = hostnames;
    }

    @Override
    public void setEnableConnectRetries(boolean enable) {
        _enableConnectRetries = enable;
    }

    @Override
    public void executeCommand(String command, OutputStream outputStream) throws IOException {
        executeCommand(_hostnames, command, outputStream);
    }

    @Override
    public void executeCommand(String command, OutputStream outputStream, int[] targetedInstances) throws IOException {
        executeCommand(getHosts(targetedInstances), command, outputStream);
    }

    private void executeCommand(List<String> hostnames, final String command, final OutputStream outputStream) throws IOException {
        executeSshCommand(hostnames, command, null, outputStream);
    }

    @Override
    public void executeCommandFile(File commandFile, OutputStream outputStream) throws IOException {
        executeSshCommand(_hostnames, null, commandFile, outputStream);
    }

    @Override
    public void executeCommandFile(File commandFile, OutputStream outputStream, int[] targetedInstances) throws IOException {
        executeSshCommand(getHosts(targetedInstances), null, commandFile, outputStream);
    }

    private void executeSshCommand(final List<String> hostnames, final String command, final File commandFile, final OutputStream outputStream) throws IOException {
        List<SshCallable> sshCallables = Lists.newArrayList();
        if (hostnames.size() == 1) {
            // don't cache the outputstream
            sshCallables.add(new SshCallable() {
                @Override
                protected void execute() throws IOException {
                    executeCommandOrCommandFile(hostnames.get(0), command, commandFile, outputStream);
                }
            });
        } else {
            // cache the outputstream for ordering the results
            for (final String host : hostnames) {
                sshCallables.add(new SshCallable() {
                    ByteArrayOutputStream _byteArrayOutputStream;

                    @Override
                    protected void execute() throws IOException {
                        _byteArrayOutputStream = new ByteArrayOutputStream();
                        executeCommandOrCommandFile(host, command, commandFile, _byteArrayOutputStream);
                    }

                    @Override
                    public void close() {
                        try {
                            outputStream.write(_byteArrayOutputStream.toByteArray());
                        } catch (IOException e) {
                            throw ExceptionUtil.convertToRuntimeException(e);
                        }
                    }
                });
            }
        }
        executeCallables(sshCallables);
    }

    private void executeCallables(List<SshCallable> sshCallables) throws IOException {
        ExecutorService e = Executors.newCachedThreadPool();
        List<Future<SshCallable>> futureList = Lists.newArrayListWithCapacity(sshCallables.size());
        for (SshCallable sshCallable : sshCallables) {
            futureList.add(e.submit(sshCallable));
        }
        waitForSshCommandCompletion(futureList);
    }

    private void executeCommandOrCommandFile(final String host, final String command, final File commandFile, OutputStream outputStream) throws IOException {
        JschRunner jschRunner = createJschRunner(host);
        if (command != null) {
            LOG.info(String.format("executing command '%s' on '%s'", command, host));
            jschRunner.run(new SshExecCommand(command, outputStream));
        } else {
            LOG.info(String.format("executing command-file '%s' on '%s'", commandFile.getAbsolutePath(), host));
            jschRunner.run(new SshExecCommand(commandFile, outputStream));
        }
    }

    private static void waitForSshCommandCompletion(List<Future<SshCallable>> futureList) throws IOException {
        boolean interrupted = false;
        for (Future<SshCallable> future : futureList) {
            try {
                if (interrupted) {
                    future.cancel(true);
                } else {
                    SshCallable sshTask = null;
                    try {
                        sshTask = future.get();
                    } finally {
                        IoUtil.closeQuietly(sshTask);
                    }
                }
            } catch (InterruptedException ex) {
                interrupted = true;
            } catch (ExecutionException ex) {
                Throwables.propagateIfInstanceOf(ex.getCause(), IOException.class);
                Throwables.propagate(ex.getCause());
            }
        }
    }

    @Override
    public void uploadFile(File localFile, String targetPath) throws IOException {
        uploadFile(_hostnames, localFile, targetPath);
    }

    @Override
    public void uploadFile(File localFile, String targetPath, int[] instanceIndex) throws IOException {
        List<String> hostnames = getHosts(instanceIndex);
        uploadFile(hostnames, localFile, targetPath);
    }

    private void uploadFile(List<String> hostnames, final File localFile, final String targetPath) throws IOException {
        List<SshCallable> callables = Lists.newArrayList();
        for (final String host : hostnames) {
            callables.add(new SshCallable() {
                @Override
                protected void execute() throws IOException {
                    LOG.info(String.format("uploading file '%s' to '%s'", localFile.getAbsolutePath(), constructRemotePath(host, targetPath)));
                    JschRunner jschRunner = createJschRunner(host);
                    jschRunner.run(new ScpUploadCommand(localFile, targetPath));
                }
            });

        }
        executeCallables(callables);
    }

    @Override
    public void downloadFile(String remoteFile, File localPath, boolean recursiv) throws IOException {
        downloadFiles(_hostnames, remoteFile, localPath, recursiv);
    }

    @Override
    public void downloadFile(String remoteFile, File localPath, boolean recursiv, int[] instanceIndex) throws IOException {
        List<String> hosts = getHosts(instanceIndex);
        downloadFiles(hosts, remoteFile, localPath, recursiv);
    }

    private void downloadFiles(List<String> hostnames, String remoteFile, File localPath, boolean recursiv) throws IOException {
        for (String host : hostnames) {
            LOG.info(String.format("downloading file '%s' to '%s'", constructRemotePath(host, remoteFile), localPath.getAbsolutePath()));
            JschRunner jschRunner = createJschRunner(host);
            jschRunner.run(new ScpDownloadCommand(remoteFile, localPath, recursiv));
        }
    }

    private String constructRemotePath(String host, String filePath) {
        return _username + ":" + "@" + host + ":" + filePath;
    }

    protected JschRunner createJschRunner(String host) {
        JschRunner runner = new JschRunner(_username, host);
        if (_privateKey != null) {
            runner.setKeyfile(_privateKey);
        } else {
            runner.setPassword(_password);
        }
        runner.setTrust(true);
        runner.setEnableConnectionRetries(_enableConnectRetries);
        return runner;
    }

    protected List<String> getHosts(int[] instanceIndex) {
        List<String> hostnames = new ArrayList<String>(_hostnames.size());
        for (int i = 0; i < instanceIndex.length; i++) {
            hostnames.add(_hostnames.get(instanceIndex[i]));
        }
        return hostnames;
    }

    private static abstract class SshCallable implements Callable<SshCallable>, Closeable {

        @Override
        public final SshCallable call() throws Exception {
            execute();
            return this;
        }

        protected abstract void execute() throws IOException;

        @Override
        public void close() {
            // subclasses may override
        };
    }

}
