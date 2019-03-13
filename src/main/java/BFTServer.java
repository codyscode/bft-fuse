


import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.defaultservices.DefaultRecoverable;
import jnr.constants.platform.OpenFlags;
import jnr.posix.POSIX;
import jnr.posix.POSIXFactory;
import ru.serce.jnrfuse.ErrorCodes;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class BFTServer extends DefaultRecoverable {

    private Logger logger;
    POSIX posix;
    String rootPath;

    public static void main(String[] args) {
        if(args.length < 2) {
            System.out.println("Use: java BFTServer <serverID> <rootPath>");
            System.exit(-1);
        }
        new BFTServer(Integer.parseInt(args[0]), args[1]);
    }

    public BFTServer(int id, String rootPath) {
        logger = Logger.getLogger(BFTServer.class.getName());
        posix = POSIXFactory.getPOSIX();
        this.rootPath = rootPath;
         new ServiceReplica(id, this, this);
    }

    @SuppressWarnings("unchecked")
    @Override
    public byte[][] appExecuteBatch(byte[][] commands, MessageContext[] msgCtx, boolean fromConsensus) {

        byte[][] replies = new byte[commands.length][];
        int index = 0;
        for (byte[] command: commands) {


            boolean hasReply = false;
            try (ByteArrayInputStream byteIn = new ByteArrayInputStream(command);
                 ObjectInput objIn = new ObjectInputStream(byteIn);
                 ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
                 ObjectOutput objOut = new ObjectOutputStream(byteOut)) {


                int result, flags;
                long mode, size, offset, uid, gid;
                FSOperation operation = (FSOperation) objIn.readObject();
                String path = (String)objIn.readObject();
                //Removes any nefarious ../ attempts, then appends to path of the "root" folder
                path = Paths.get(path).normalize().toString();
                path = rootPath + path;
                switch (operation) {


                    case MKDIR:
                        logger.log(Level.INFO, "Called MKDIR with path: " + path);
                        mode = (long) objIn.readObject();
                        result = posix.mkdir(path, (int) mode);
                        if (result < 0) {
                            result = -posix.errno();
                        }
                        objOut.writeObject(result);
                        hasReply = true;
                        break;

                    case RMDIR:
                        logger.log(Level.INFO, "Called RMDIR with path: " + path);
                        result = posix.rmdir(path);
                        if (result < 0) {
                            result = -posix.errno();
                        }
                        objOut.writeObject(result);
                        hasReply = true;
                        break;

                    case UTIMES:
                        logger.log(Level.INFO, "Called UTIMES with path: " + path);
                        long[] atimeval = (long[]) objIn.readObject();
                        long[] mtimeval = (long[]) objIn.readObject();
                        result = posix.utimes(path, atimeval, mtimeval);
                        if (result < 0) {
                            result = -posix.errno();
                        }
                        objOut.writeObject(result);
                        hasReply = true;
                        break;

                    case RENAME:
                        logger.log(Level.INFO, "Called RENAME with path: " + path);
                        String newPath = rootPath + (String) objIn.readObject();
                        result = posix.rename(path, newPath);
                        if (result < 0) {
                            result = -posix.errno();
                        }
                        objOut.writeObject(result);
                        hasReply = true;
                        break;

                    case CHMOD:
                        logger.log(Level.INFO, "Called CHMOD with path: " + path);
                        mode = (long) objIn.readObject();
                        result = posix.chmod(path, (int) mode);
                        if (result < 0) {
                            result = -posix.errno();
                        }
                        objOut.writeObject(result);
                        hasReply = true;
                        break;

                    case CHOWN:
                        logger.log(Level.INFO, "Called CHOWN with path: " + path);
                        uid = (long) objIn.readObject();
                        gid = (long) objIn.readObject();
                        result = posix.chown(path, (int) uid, (int) gid);
                        if (result < 0) {
                            result = -posix.errno();
                        }
                        objOut.writeObject(result);
                        hasReply = true;
                        break;

                    case UNLINK:
                        logger.log(Level.INFO, "Called UNLINK with path: " + path);
                        result = posix.unlink(path);
                        if (result < 0) {
                            result = -posix.errno();
                        }
                        objOut.writeObject(result);
                        hasReply = true;
                        break;

                    case CREATE:
                        logger.log(Level.INFO, "Called CREATE with path: " + path);
                        mode = (long) objIn.readObject();
                        if (new File(path).exists()) {
                            result = -ErrorCodes.EEXIST();
                        } else {
                            //open with these flags is equivalent to create
                            result = posix.open(path, OpenFlags.O_CREAT.intValue() | OpenFlags.O_WRONLY.intValue() | OpenFlags.O_TRUNC.intValue(), (int) mode);
                            if (result < 0) {
                                result = -posix.errno();
                            } else {
                                posix.close(result);
                                //don't want the file descriptor
                                result = 0;
                            }
                        }
                        objOut.writeObject(result);
                        hasReply = true;
                        break;

                    case TRUNCATE:
                        logger.log(Level.INFO, "Called TRUNCATE with path: " + path);
                        size = (long) objIn.readObject();
                        result = posix.truncate(path, size);
                        if (result < 0) {
                            result = -posix.errno();
                        }
                        objOut.writeObject(result);
                        hasReply = true;
                        break;

                    case OPEN:
                        logger.log(Level.INFO, "Called OPEN with path: " + path);
                        flags = (int) objIn.readObject();
                        result = posix.open(path, flags, OpenFlags.O_RDONLY.intValue());
                        if (result < 0) {
                            result = -posix.errno();
                        } else {
                            posix.close(result);
                            //don't care about file descriptors and the ones returned mess things up
                            result = 0;
                        }
                        objOut.writeObject(result);
                        hasReply = true;
                        break;

                    case READ:
                        logger.log(Level.INFO, "Called READ with path: " + path);
                        size = (long) objIn.readObject();
                        offset = (long) objIn.readObject();
                        flags = (int) objIn.readObject();
                        byte[] buffer = new byte[(int) size];
                        int fd = posix.open(path, flags, OpenFlags.O_RDONLY.intValue());
                        if (fd < 0) {
                            result = -posix.errno();
                        } else {
                            result = (int) posix.pread(fd, buffer, size, offset);
                            if (result < 0) {
                                result = -posix.errno();
                            } else if (result < size) {
                                //have to trim garbage when fuse defaults size to 4096 or consensus will be broken
                                buffer = Arrays.copyOf(buffer, result);
                            }
                            posix.close(fd);
                        }
                        objOut.writeObject(result);
                        objOut.writeObject(buffer);
                        hasReply = true;
                        break;

                    case WRITE:
                        logger.log(Level.INFO, "Called WRITE with path: " + path);
                        buffer = (byte[]) objIn.readObject();
                        size = (long) objIn.readObject();
                        offset = (long) objIn.readObject();
                        flags = (int) objIn.readObject();
                        fd = posix.open(path, flags, OpenFlags.O_RDWR.intValue());
                        if (fd < 0) {
                            result = -posix.errno();
                        } else {
                            result = (int) posix.pwrite(fd, buffer, size, offset);
                            if (result < 0) {
                                result = -posix.errno();
                            }
                            posix.close(fd);
                        }
                        objOut.writeObject(result);
                        hasReply = true;
                        break;

                    case GETATTR:
                        logger.log(Level.INFO, "Called GETATTR with path: " + path);
                        jnr.posix.FileStat jnrStat = posix.allocateStat();
                        result = posix.lstat(path, jnrStat);
                        if (result < 0) {
                            result = -posix.errno();
                        }
                        objOut.writeObject(result);
                        //If result != 0 jnrStat will be random garbage and mess up the consensus, so don't send
                        if (result == 0) {
                            //Omitting atime, ctime, dev, ino, mtime, rdev, since they can break consensus
                            objOut.writeObject(jnrStat.blocks()); //long
                            objOut.writeObject(jnrStat.blockSize()); //long
                            objOut.writeObject(jnrStat.gid()); //int
                            objOut.writeObject(jnrStat.mode()); //int
                            objOut.writeObject(jnrStat.nlink()); //int
                            objOut.writeObject(jnrStat.st_size()); //long
                            objOut.writeObject(jnrStat.uid()); //int
                        }
                        hasReply = true;
                        break;

                    case READDIR:
                        logger.log(Level.INFO, "Called READDIR with path: " + path);
                        File dir = new File(path);
                        if (!dir.exists()) {
                            result = -ErrorCodes.ENOENT();
                        } else {
                            result = 0;
                        }
                        String[] filenames = dir.list();
                        //Since filenames aren't guaranteed an order
                        Arrays.sort(filenames);
                        objOut.writeObject(result);
                        objOut.writeObject(filenames);
                        hasReply = true;
                        break;

                }
                if (hasReply) {
                    objOut.flush();
                    byteOut.flush();
                    replies[index++] = byteOut.toByteArray();
                } else {
                    replies[index++] = new byte[0];
                }

            } catch (IOException | ClassNotFoundException ex) {
                logger.log(Level.SEVERE, null, ex);
            }
        }
        return replies;
    }

    // Will move some read operations here when performance becomes an issue
    @SuppressWarnings("unchecked")
    @Override
    public byte[] appExecuteUnordered(byte[] command, MessageContext msgCtx) {
        byte[] reply = null;
        boolean hasReply = false;
        try (ByteArrayInputStream byteIn = new ByteArrayInputStream(command);
             ObjectInput objIn = new ObjectInputStream(byteIn);
             ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
             ObjectOutput objOut = new ObjectOutputStream(byteOut)) {

            String path;
            int result;
            FSOperation operation = (FSOperation)objIn.readObject();
            switch (operation) {

            }
            if (hasReply) {
                objOut.flush();
                byteOut.flush();
                reply = byteOut.toByteArray();
            } else {
                reply = new byte[0];
            }

        } catch (IOException | ClassNotFoundException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        return reply;
    }

    @Override
    public byte[] getSnapshot() {
        logger.log(Level.INFO, "Called getSnapshot");
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
             ObjectOutput objOut = new ObjectOutputStream(byteOut)) {

            List<Path> pathWalk = Files.walk(Paths.get(rootPath)).collect(Collectors.toList());
            Collections.sort(pathWalk);
            objOut.writeObject(pathWalk.size());
            for (Path path : pathWalk) {
                String absolutePath = path.toAbsolutePath().toString();
                String relativeRoot = absolutePath.replace(rootPath, "");
                jnr.posix.FileStat jnrStat = posix.allocateStat();
                if (posix.lstat(absolutePath, jnrStat) < 0) {
                    logger.log(Level.SEVERE, "Error getting FileStat in getSnapshot");
                }
                objOut.writeObject(relativeRoot);
                objOut.writeObject(jnrStat.gid()); //int
                objOut.writeObject(jnrStat.isDirectory()); //boolean
                objOut.writeObject(jnrStat.mode()); //int
                objOut.writeObject(jnrStat.uid()); //int

                if (path.toFile().isFile()) {
                    byte[] contents = Files.readAllBytes(path);
                    objOut.writeObject(contents);
                }
            }
            objOut.flush();
            byteOut.flush();
            return byteOut.toByteArray();

        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error while taking snapshot", e);
        }
        return new byte[0];
    }

    @SuppressWarnings("unchecked")
    @Override
    public void installSnapshot(byte[] state) {
        logger.log(Level.INFO, "Called installSnapshot");
        try (ByteArrayInputStream byteIn = new ByteArrayInputStream(state);
             ObjectInput objIn = new ObjectInputStream(byteIn)) {

            //Clear out old state
            String[] args = {"rm", "-rf", rootPath };
            Process process = java.lang.Runtime.getRuntime().exec(args);
            process.waitFor();

            int entryCount = (int)objIn.readObject();
            for (int i = 0; i < entryCount; i++) {

                String absolutePath = rootPath + (String)objIn.readObject();
                int gid = (int)objIn.readObject();
                boolean isDirectory = (boolean)objIn.readObject();
                int mode = (int)objIn.readObject();
                int uid = (int)objIn.readObject();

                File entry = new File(absolutePath);
                if (isDirectory) {
                    entry.mkdirs();
                } else {
                    entry.getParentFile().mkdirs();
                    entry.createNewFile();
                    byte[] contents = (byte[])objIn.readObject();
                    try (FileOutputStream fos = new FileOutputStream(absolutePath)) {
                        fos.write(contents);
                    }
                }
                posix.chmod(absolutePath, mode);
                posix.chown(absolutePath, uid, gid);
            }
        } catch (IOException | ClassNotFoundException e) {
            logger.log(Level.SEVERE, "Error while installing snapshot", e);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

