import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.defaultservices.DefaultRecoverable;
import jnr.constants.platform.OpenFlags;
import jnr.posix.POSIX;
import jnr.posix.POSIXFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.serce.jnrfuse.ErrorCodes;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class BFTServer extends DefaultRecoverable {

    private Logger logger;
    POSIX posix;
    String replicaPath;

    public static void main(String[] args) {
        if(args.length < 2) {
            System.out.println("Use: java BFTServer <serverID> <replicaPath>");
            System.exit(-1);
        }
        new BFTServer(Integer.parseInt(args[0]), args[1]);
    }

    public BFTServer(int id, String replicaPath) {
        logger = LoggerFactory.getLogger(this.getClass());
        try {
            this.replicaPath = Paths.get(replicaPath).toRealPath().toString();
        } catch (IOException e) {
            logger.error("Unable to resolve provided path: ", e);
            System.exit(-1);
        }
        posix = POSIXFactory.getPOSIX();
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
                path = replicaPath + path;
                switch (operation) {

                    case MKDIR:
                        logger.debug("Called MKDIR with path: " + path);
                        mode = (long) objIn.readObject();
                        result = posix.mkdir(path, (int) mode);
                        if (result < 0) {
                            result = -posix.errno();
                        }
                        objOut.writeObject(result);
                        hasReply = true;
                        break;

                    case RMDIR:
                        logger.debug("Called RMDIR with path: " + path);
                        result = posix.rmdir(path);
                        if (result < 0) {
                            result = -posix.errno();
                        }
                        objOut.writeObject(result);
                        hasReply = true;
                        break;

                    case UTIMES:
                        logger.debug("Called UTIMES with path: " + path);
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
                        logger.debug("Called RENAME with path: " + path);
                        String newPath = replicaPath + (String) objIn.readObject();
                        result = posix.rename(path, newPath);
                        if (result < 0) {
                            result = -posix.errno();
                        }
                        objOut.writeObject(result);
                        hasReply = true;
                        break;

                    case CHMOD:
                        logger.debug("Called CHMOD with path: " + path);
                        mode = (long) objIn.readObject();
                        result = posix.chmod(path, (int) mode);
                        if (result < 0) {
                            result = -posix.errno();
                        }
                        objOut.writeObject(result);
                        hasReply = true;
                        break;

                    case CHOWN:
                        logger.debug("Called CHOWN with path: " + path);
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
                        logger.debug("Called UNLINK with path: " + path);
                        result = posix.unlink(path);
                        if (result < 0) {
                            result = -posix.errno();
                        }
                        objOut.writeObject(result);
                        hasReply = true;
                        break;

                    case CREATE:
                        logger.debug("Called CREATE with path: " + path);
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
                        logger.debug("Called TRUNCATE with path: " + path);
                        size = (long) objIn.readObject();
                        result = posix.truncate(path, size);
                        if (result < 0) {
                            result = -posix.errno();
                        }
                        objOut.writeObject(result);
                        hasReply = true;
                        break;

                    case OPEN:
                        logger.debug("Called OPEN with path: " + path);
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
                        logger.debug("Called READ with path: " + path);
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
                        logger.debug("Called WRITE with path: " + path);
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
                        logger.debug("Called GETATTR with path: " + path);
                        jnr.posix.FileStat jnrStat = posix.allocateStat();
                        result = posix.lstat(path, jnrStat);
                        if (result < 0) {
                            result = -posix.errno();
                        }
                        objOut.writeObject(result);
                        //If result != 0 jnrStat will be random garbage and mess up the consensus, so don't send
                        if (result == 0) {
                            //Omitting atime, ctime, blocks, blockSize, mtime, dev, rdev, ino, since they can break
                            //consensus. Some of these could potentially be added back depending on the configuration
                            objOut.writeObject(jnrStat.gid()); //int
                            objOut.writeObject(jnrStat.mode()); //int
                            objOut.writeObject(jnrStat.nlink()); //int
                            objOut.writeObject(jnrStat.st_size()); //long
                            objOut.writeObject(jnrStat.uid()); //int
                        }
                        hasReply = true;
                        break;

                    case READDIR:
                        logger.debug("Called READDIR with path: " + path);
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
                logger.error(null, ex);
            }
        }
        return replies;
    }

    // Will move some read operations here when performance becomes an issue
    // Certain read operations are invoked immediately before writes so this could be tricky
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
            logger.error(null, ex);
        }
        return reply;
    }

    //Serializes the entire filesystem into a byte array
    //Limited by max size of byte array, will possibly change in the future
    @Override
    public byte[] getSnapshot() {
        logger.debug("Called getSnapshot");
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
             ObjectOutput objOut = new ObjectOutputStream(byteOut)) {

            List<Path> pathWalk = Files.walk(Paths.get(replicaPath)).collect(Collectors.toList());
            Collections.sort(pathWalk);
            objOut.writeObject(pathWalk.size());
            for (Path path : pathWalk) {
                String absolutePath = path.toAbsolutePath().toString();
                String relativeRoot = absolutePath.replace(replicaPath, "");
                jnr.posix.FileStat jnrStat = posix.allocateStat();
                if (posix.lstat(absolutePath, jnrStat) < 0) {
                    logger.error("Error getting FileStat in getSnapshot");
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
            logger.error("Error while taking snapshot", e);
        }
        return new byte[0];
    }

    //Deletes current filesystem, deserializes byte array, and installs filesystem from getSnapshot
    @SuppressWarnings("unchecked")
    @Override
    public void installSnapshot(byte[] state) {
        logger.debug("Called installSnapshot");
        try (ByteArrayInputStream byteIn = new ByteArrayInputStream(state);
             ObjectInput objIn = new ObjectInputStream(byteIn)) {

            //Clear out old state
            String[] args = {"rm", "-rf", replicaPath };
            Process process = java.lang.Runtime.getRuntime().exec(args);
            process.waitFor();

            int entryCount = (int)objIn.readObject();
            for (int i = 0; i < entryCount; i++) {

                String absolutePath = replicaPath + (String)objIn.readObject();
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
            logger.error("Error while installing snapshot", e);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

