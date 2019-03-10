


import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.defaultservices.DefaultRecoverable;
import bftsmart.tom.server.defaultservices.DefaultSingleRecoverable;
import jnr.constants.platform.OpenFlags;
import jnr.posix.POSIX;
import jnr.posix.POSIXFactory;
import ru.serce.jnrfuse.ErrorCodes;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class BFTServer extends DefaultSingleRecoverable {

    private Logger logger;
    POSIX posix;
    String rootPath;

    public BFTServer(int id, String rootPath) {
        logger = Logger.getLogger(BFTServer.class.getName());
        posix = POSIXFactory.getPOSIX();
        this.rootPath = rootPath;
         new ServiceReplica(id, this, this);
    }

    public static void main(String[] args) {
        if(args.length < 1) {
            System.out.println("Use: java BFTServer <processId>");
            System.exit(-1);
        }
        new BFTServer(Integer.parseInt(args[0]), args[1]);

    }



    @SuppressWarnings("unchecked")
    @Override
    public byte[] appExecuteOrdered(byte[] command, MessageContext msgCtx) {
        System.out.println("Called appExecuteOrdered in BFTServer");
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

                case GETATTR:
                    path = rootPath + (String)objIn.readObject();
                    System.out.printf("Called GETATTR case in appExecuteUnordered in BFTServer with path %s\n", path);
                    jnr.posix.FileStat jnrStat = posix.allocateStat();
                    result = posix.lstat(path, jnrStat);
                    if (result < 0) {
                        result = -posix.errno();
                    }
                    //This is messy but easier than making serializable versions
                    //of all subclasses.
                    objOut.writeObject(result);
                    System.out.printf("Wrote result out with value of %d\n", result);
                    //If result != 0 jnrStat will be random garbage and mess up the consensus
                    if (result == 0) {
                        //objOut.writeObject(jnrStat.atime());  //long
                        objOut.writeObject(jnrStat.blocks()); //long
                        System.out.printf("Wrote blocks with value of %d\n", jnrStat.blocks());
                        objOut.writeObject(jnrStat.blockSize()); //long
                        System.out.printf("Wrote bloxkSize with value of %d\n", jnrStat.blockSize());
                        //objOut.writeObject(jnrStat.ctime()); //long
                        //objOut.writeObject(jnrStat.dev()); //long
                        objOut.writeObject(jnrStat.gid()); //int
                        System.out.printf("Wrote gid out with value of %d\n", jnrStat.gid());
                        //objOut.writeObject(jnrStat.ino()); //long
                        objOut.writeObject(jnrStat.mode()); //int
                        System.out.printf("Wrote mode out with value of %d\n", jnrStat.mode());
                        //objOut.writeObject(jnrStat.mtime()); //long
                        objOut.writeObject(jnrStat.nlink()); //int
                        System.out.printf("Wrote nlink out with value of %d\n", jnrStat.nlink());
                        //objOut.writeObject(jnrStat.rdev()); //long
                        objOut.writeObject(jnrStat.st_size()); //long
                        System.out.printf("Wrote st_size out with value of %d\n", jnrStat.st_size());
                        objOut.writeObject(jnrStat.uid()); //int
                        //System.out.printf("Wrote uid out with value of %d\n", jnrStat.uid());
                    }
                    hasReply = true;
                    break;


                case READDIR:
                    path = rootPath + (String)objIn.readObject();
                    System.out.printf("Called READDIR case in appExecuteUnordered in BFTServer with path %s\n", path);
                    File dir = new File(path);
                    if (!dir.exists()) {
                        result = -ErrorCodes.ENOENT();
                    } else {
                        result = 0;
                    }
                    String[] filenames = new File(path).list();
                    //Since filenames aren't guaranteed an order;
                    Arrays.sort(filenames);
                    objOut.writeObject(result);
                    objOut.writeObject(filenames);
                    hasReply = true;
                    break;


                case MKDIR:
                    path = rootPath + (String)objIn.readObject();
                    System.out.printf("Called MKDIR case in appExecuteOordered in BFTServer with path: %s\n", path);
                    long mode = (long)objIn.readObject();
                    result = posix.mkdir(path, (int)mode);
                    if (result < 0) {
                        result = -posix.errno();
                    }
                    objOut.writeObject(result);
                    hasReply = true;
                    break;

                case RMDIR:
                    path = rootPath + (String)objIn.readObject();
                    System.out.printf("Called RMDIR case in appExecuteOrdered in BFTServer with path: %s\n", path);
                    result = posix.rmdir(path);
                    if (result < 0) {
                        result = -posix.errno();
                    }
                    objOut.writeObject(result);
                    hasReply = true;
                    break;

                case UTIMES:
                    path = rootPath + (String)objIn.readObject();
                    System.out.printf("Called UTIMENS case in appExecuteOnordered in BFTServer with path: %s\n", path);
                    long[] atimeval = (long[])objIn.readObject();
                    long[] mtimeval = (long[])objIn.readObject();
                    result = posix.utimes(path, atimeval, mtimeval);
                    if (result < 0) {
                        result = -posix.errno();
                    }
                    objOut.writeObject(result);
                    hasReply = true;
                    break;

                case RENAME:
                    String oldPath = rootPath + (String)objIn.readObject();
                    String newPath = rootPath + (String)objIn.readObject();
                    System.out.printf("Called RENAME case in BFTServer with old path %s  newpath %s\n", oldPath, newPath);
                    result = posix.rename(oldPath, newPath);
                    if (result < 0) {
                        result = -posix.errno();
                    }
                    objOut.writeObject(result);
                    hasReply = true;
                    break;

                case CHMOD:
                    path = rootPath + (String)objIn.readObject();
                    System.out.printf("Called CHMOD case in BFTServer with path %s\n", path);
                    mode = (long)objIn.readObject();
                    result = posix.chmod(path, (int)mode);
                    if (result < 0) {
                        result = -posix.errno();
                    }
                    objOut.writeObject(result);
                    hasReply = true;
                    break;

                case CHOWN:
                    path = rootPath + (String)objIn.readObject();
                    System.out.printf("Called CHOWN case in BFTServer with path %s\n", path);
                    long uid = (long)objIn.readObject();
                    long gid = (long)objIn.readObject();
                    result = posix.chown(path, (int)uid, (int)gid);
                    if (result < 0) {
                        result = -posix.errno();
                    }
                    objOut.writeObject(result);
                    hasReply = true;
                    break;

                case UNLINK:
                    path = rootPath + (String)objIn.readObject();
                    System.out.printf("Called UNLINK case in BFTServer with path %s\n", path);
                    result = posix.unlink(path);
                    if (result < 0) {
                        result = -posix.errno();
                    }
                    objOut.writeObject(result);
                    hasReply = true;
                    break;

                case CREATE:
                    path = rootPath + (String)objIn.readObject();
                    System.out.printf("Called CREATE case in BFTServer with path %s\n", path);
                    mode = (long)objIn.readObject();
                    if (new File(path).exists()) {
                        result = -ErrorCodes.EEXIST();
                    } else {
                        //open with these flags is equivalent to create
                        result = posix.open(path, OpenFlags.O_CREAT.intValue() | OpenFlags.O_WRONLY.intValue() | OpenFlags.O_TRUNC.intValue(), (int)mode);
                        if (result < 0) {
                            result = -posix.errno();
                        } else {
                            posix.close(result);
                            result = 0; //don't want the file descriptor
                        }
                    }
                    objOut.writeObject(result);
                    hasReply = true;
                    break;

                case TRUNCATE:
                    path = rootPath + (String)objIn.readObject();
                    System.out.printf("Called TRUNCATE in BFTServer with path %s\n", path);
                    long size = (long)objIn.readObject();
                    result = posix.truncate(path, size);
                    if (result < 0) {
                        result = -posix.errno();
                    }
                    objOut.writeObject(result);
                    hasReply = true;
                    break;

                case OPEN:
                    path = rootPath + (String)objIn.readObject();
                    System.out.printf("Called OPEN in BFTServer with path %s\n", path);
                    int flags = (int)objIn.readObject();
                    result = posix.open(path, flags, OpenFlags.O_RDONLY.intValue());
                    if (result < 0) {
                        result = -posix.errno();
                    } else {
                        posix.close(result);
                        result = 0; //don't care about file descriptors and the ones returned seem to mess things up
                    }
                    objOut.writeObject(result);
                    hasReply = true;
                    break;


                case READ:
                    path = rootPath + (String)objIn.readObject();
                    System.out.printf("Called READ case in BFTServer with path %s\n", path);
                    size = (long)objIn.readObject();
                    long offset = (long)objIn.readObject();
                    flags = (int)objIn.readObject();
                    byte[] buffer = new byte[(int)size];
                    int fd = posix.open(path, flags, OpenFlags.O_RDONLY.intValue());
                    if (fd < 0) {
                        result = -posix.errno();
                    } else {
                        result =  (int)posix.pread(fd, buffer, size, offset);
                        //have to trim garbage when fuse defaults size to 4096 or consensus will be broken
                        if (result < size) {
                            buffer = Arrays.copyOf(buffer, result);
                        }
                        System.out.printf("The result value of pread in BFTServer in read is %d\n", result);
                        System.out.printf("The size of buffer in BFTServer in read is %d\n", size);
                        System.out.printf("The buffer in BFTServer in read contains %s\n", new String(buffer));
                        if (result < 0) {
                            result = -posix.errno();
                        }
                        posix.close(fd);
                    }
                    objOut.writeObject(result);
                    objOut.writeObject(buffer);
                    hasReply = true;
                    break;


                case WRITE:
                    path = rootPath + (String)objIn.readObject();
                    System.out.printf("Called WRITE case in BFTServer with path: %s\n", path);
                    buffer = (byte[])objIn.readObject();
                    System.out.printf("The buffer in BFTServer in write contains %s\n", new String(buffer));
                    size = (long)objIn.readObject();
                    offset = (long)objIn.readObject();
                    flags = (int)objIn.readObject();
                    fd = posix.open(path, flags, OpenFlags.O_RDWR.intValue());
                    if (fd < 0) {
                        result = -posix.errno();
                    } else {
                        result = (int)posix.pwrite(fd, buffer, size, offset);
                        if (result < 0) {
                            result = -posix.errno();
                        }
                        posix.close(fd);
                    }
                    objOut.writeObject(result);
                    hasReply = true;
                    break;




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

    @SuppressWarnings("unchecked")
    @Override
    public byte[] appExecuteUnordered(byte[] command, MessageContext msgCtx) {
        System.out.println("Called appExecuteUnordered in BFTServer");
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
        System.out.println("Called getSnapshot");

        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
             ObjectOutput objOut = new ObjectOutputStream(byteOut)) {
            //objOut.writeObject(replicaMap);


            List<Path> pathWalk = Files.walk(Paths.get(rootPath)).collect(Collectors.toList());
            Collections.sort(pathWalk);
            objOut.writeObject(pathWalk.size());
            for (Path path : pathWalk) {
                String absolutePath = path.toAbsolutePath().toString();
                String relativeRoot = absolutePath.replace(rootPath, "");
                objOut.writeObject(relativeRoot);
                System.out.printf("Writing pathname: %s\n", relativeRoot);
                jnr.posix.FileStat jnrStat = posix.allocateStat();
                if (posix.lstat(absolutePath, jnrStat) < 0) {
                    logger.log(Level.SEVERE, "Error getting FileStat in getSnapshot");
                }
                //objOut.writeObject(jnrStat.blocks()); //long
                //objOut.writeObject(jnrStat.blockSize()); //long
                objOut.writeObject(jnrStat.gid()); //int
                System.out.printf("The gid for this is %s\n", jnrStat.gid());
                objOut.writeObject(jnrStat.mode()); //int
                System.out.printf("The mode for this is %s\n", jnrStat.mode());
                //objOut.writeObject(jnrStat.nlink()); //int
                //objOut.writeObject(jnrStat.st_size()); //long
                objOut.writeObject(jnrStat.uid()); //int
                System.out.printf("The uid for this is %s\n", jnrStat.uid());
                if (path.toFile().isFile()) {
                    System.out.println("This is a file, writing contents");
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
        System.out.println("Called installSnapshot");
        try (ByteArrayInputStream byteIn = new ByteArrayInputStream(state);
             ObjectInput objIn = new ObjectInputStream(byteIn)) {

            //Clear out old state
            String[] args = {"rm", "-rf", rootPath };
            Process process = java.lang.Runtime.getRuntime().exec(args);
            process.waitFor();

            int entryCount = (int)objIn.readObject();
            for (int i = 0; i < entryCount; i++) {
                String relativeRoot = (String)objIn.readObject();
                String absolutePath = rootPath + relativeRoot;
                System.out.println("Creating new entry at absolute path: " + absolutePath);
                int gid = (int)objIn.readObject();
                int mode = (int)objIn.readObject();
                //int nlink = (int)objIn.readObject();
                int uid = (int)objIn.readObject();
                posix.mkdir(absolutePath, mode);
                posix.chown(absolutePath, uid, gid);
                if(Paths.get(absolutePath).toFile().isFile()){
                    byte[] content = (byte[])objIn.readObject();
                    try (FileOutputStream fos = new FileOutputStream(absolutePath)) {
                        fos.write(content);
                    }
                }
            }
        } catch (IOException | ClassNotFoundException e) {
            logger.log(Level.SEVERE, "Error while installing snapshot", e);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}

