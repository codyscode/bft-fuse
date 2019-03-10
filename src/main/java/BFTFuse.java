

import bftsmart.tom.ServiceProxy;
import jnr.ffi.Platform;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import jnr.ffi.types.mode_t;
import jnr.ffi.types.off_t;
import jnr.ffi.types.size_t;
import jnr.posix.POSIX;
import jnr.posix.POSIXFactory;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;
import ru.serce.jnrfuse.struct.Statvfs;
import ru.serce.jnrfuse.struct.Timespec;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;


public class BFTFuse extends FuseStubFS {

    Logger logger;
    POSIX posix;
    ServiceProxy serviceProxy;

    public BFTFuse(int clientID) {
        logger = Logger.getLogger(BFTFuse.class.getName());
        posix = POSIXFactory.getPOSIX();
        serviceProxy = new ServiceProxy(clientID, "config");
    }

    @Override
    public int getattr(String path, FileStat stat) {
        System.out.println("Called getattr in BFTFuse");
        int result;
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
             ObjectOutput objOut = new ObjectOutputStream(byteOut)) {
            objOut.writeObject(FSOperation.GETATTR);
            objOut.writeObject(path);
            objOut.flush();
            byteOut.flush();
            byte[] reply = serviceProxy.invokeOrdered(byteOut.toByteArray());
            System.out.printf("Sent path %s to getattr and the reply length is %d\n", path, reply.length);
            if (reply.length == 0) {
                return -ErrorCodes.EIO();
            }
            try (ByteArrayInputStream byteIn = new ByteArrayInputStream(reply);
                 ObjectInput objIn = new ObjectInputStream(byteIn)) {
                result = (int)objIn.readObject();
                if (result == 0) {


                    //stat.st_atim.tv_sec.set((long)objIn.readObject());
                    stat.st_blocks.set((long)objIn.readObject());
                    stat.st_blksize.set((long)objIn.readObject());
                    //stat.st_ctim.tv_sec.set((long)objIn.readObject());
                    //stat.st_dev.set((long)objIn.readObject());
                    stat.st_gid.set((int)objIn.readObject());
                    //stat.st_ino.set((long)objIn.readObject());
                    stat.st_mode.set((int) objIn.readObject());
                    //stat.st_mtim.tv_sec.set((long)objIn.readObject());
                    stat.st_nlink.set((int) objIn.readObject());
                    //stat.st_rdev.set((long)objIn.readObject());
                    stat.st_size.set((long)objIn.readObject());
                    stat.st_uid.set((int)objIn.readObject());
                }

            }
        }catch (IOException | ClassNotFoundException ex) {
            logger.log(Level.SEVERE, null, ex);
            result = -ErrorCodes.EIO();

        }
        return result;
    }

    @Override
    public int mkdir(String path, @mode_t long mode) {
        System.out.println("Called mkdir in BFTFuse");
        int result;
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
             ObjectOutput objOut = new ObjectOutputStream(byteOut)) {
            objOut.writeObject(FSOperation.MKDIR);
            objOut.writeObject(path);
            objOut.writeObject(mode);
            objOut.flush();
            byteOut.flush();
            byte[] reply = serviceProxy.invokeOrdered(byteOut.toByteArray());
            System.out.printf("Sent path %s to mkdir and the reply is length: %d\n", path, reply.length);
            if (reply.length == 0) {
                return -ErrorCodes.EIO();
            }
            try (ByteArrayInputStream byteIn = new ByteArrayInputStream(reply);
                 ObjectInput objIn = new ObjectInputStream(byteIn)) {
                result = (int)objIn.readObject();
            }
        } catch (IOException | ClassNotFoundException ex) {
            logger.log(Level.SEVERE, null, ex);
            result = -ErrorCodes.EIO();

        }
        return result;
    }


    @Override
    public int rename(String oldpath, String newpath) {
        System.out.println("Called rename in BFTFuse");
        int result;
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
             ObjectOutput objOut = new ObjectOutputStream(byteOut)) {
            objOut.writeObject(FSOperation.RENAME);
            objOut.writeObject(oldpath);
            objOut.writeObject(newpath);
            objOut.flush();
            byteOut.flush();
            byte[] reply = serviceProxy.invokeOrdered(byteOut.toByteArray());
            if (reply.length == 0) {
                return -ErrorCodes.EIO();
            }
            try (ByteArrayInputStream byteIn = new ByteArrayInputStream(reply);
                 ObjectInput objIn = new ObjectInputStream(byteIn)) {
                result = (int)objIn.readObject();
            }
        } catch (IOException | ClassNotFoundException ex) {
            logger.log(Level.SEVERE, null, ex);
            result = -ErrorCodes.EIO();

        }
        return result;
    }

    @Override
    public int unlink(String path) {
        System.out.println("Called unlink in BFTFuse");
        int result;
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
             ObjectOutput objOut = new ObjectOutputStream(byteOut)) {
            objOut.writeObject(FSOperation.UNLINK);
            objOut.writeObject(path);
            objOut.flush();
            byteOut.flush();
            byte[] reply = serviceProxy.invokeOrdered(byteOut.toByteArray());
            if (reply.length == 0) {
                return -ErrorCodes.EIO();
            }
            try (ByteArrayInputStream byteIn = new ByteArrayInputStream(reply);
                 ObjectInput objIn = new ObjectInputStream(byteIn)) {
                result = (int)objIn.readObject();
            }
        } catch (IOException | ClassNotFoundException ex) {
            logger.log(Level.SEVERE, null, ex);
            result = -ErrorCodes.EIO();

        }
        return result;
    }

    @Override
    public int truncate(String path, long size) {
        System.out.println("Called truncate in BFTFuse");
        int result;
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
             ObjectOutput objOut = new ObjectOutputStream(byteOut)) {
            objOut.writeObject(FSOperation.TRUNCATE);
            objOut.writeObject(path);
            objOut.writeObject(size);
            objOut.flush();
            byteOut.flush();
            byte[] reply = serviceProxy.invokeOrdered(byteOut.toByteArray());
            if (reply.length == 0) {
                return -ErrorCodes.EIO();
            }
            try (ByteArrayInputStream byteIn = new ByteArrayInputStream(reply);
                 ObjectInput objIn = new ObjectInputStream(byteIn)) {
                result = (int)objIn.readObject();
            }
        } catch (IOException | ClassNotFoundException ex) {
            logger.log(Level.SEVERE, null, ex);
            result = -ErrorCodes.EIO();

        }
        return result;
    }

    @Override
    public int open(String path, FuseFileInfo fi) {
        System.out.println("Called open in BFTFuse");
        int result;
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
             ObjectOutput objOut = new ObjectOutputStream(byteOut)) {
            objOut.writeObject(FSOperation.OPEN);
            objOut.writeObject(path);
            objOut.writeObject(fi.flags.intValue());
            objOut.flush();
            byteOut.flush();
            byte[] reply = serviceProxy.invokeOrdered(byteOut.toByteArray());
            if (reply.length == 0) {
                return -ErrorCodes.EIO();
            }
            try (ByteArrayInputStream byteIn = new ByteArrayInputStream(reply);
                 ObjectInput objIn = new ObjectInputStream(byteIn)) {
                result = (int)objIn.readObject();
            }
        } catch (IOException | ClassNotFoundException ex) {
            logger.log(Level.SEVERE, null, ex);
            result = -ErrorCodes.EIO();

        }
        return result;
    }

    @Override
    public int create(String path, @mode_t long mode, FuseFileInfo fi) {
        System.out.println("Called create in BFTFuse");
        int result;
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
             ObjectOutput objOut = new ObjectOutputStream(byteOut)) {
            objOut.writeObject(FSOperation.CREATE);
            objOut.writeObject(path);
            objOut.writeObject(mode);
            objOut.flush();
            byteOut.flush();
            byte[] reply = serviceProxy.invokeOrdered(byteOut.toByteArray());
            if (reply.length == 0) {
                return -ErrorCodes.EIO();
            }
            try (ByteArrayInputStream byteIn = new ByteArrayInputStream(reply);
                 ObjectInput objIn = new ObjectInputStream(byteIn)) {
                result = (int)objIn.readObject();
            }
        } catch (IOException | ClassNotFoundException ex) {
            logger.log(Level.SEVERE, null, ex);
            result = -ErrorCodes.EIO();

        }
        return result;
    }

    @Override
    public int chmod(String path, long mode) {
        System.out.println("Called chmod in BFTFuse");
        int result;
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
             ObjectOutput objOut = new ObjectOutputStream(byteOut)) {
            objOut.writeObject(FSOperation.CHMOD);
            objOut.writeObject(path);
            objOut.writeObject(mode);
            objOut.flush();
            byteOut.flush();
            byte[] reply = serviceProxy.invokeOrdered(byteOut.toByteArray());
            if (reply.length == 0) {
                return -ErrorCodes.EIO();
            }
            try (ByteArrayInputStream byteIn = new ByteArrayInputStream(reply);
                 ObjectInput objIn = new ObjectInputStream(byteIn)) {
                result = (int)objIn.readObject();
            }
        } catch (IOException | ClassNotFoundException ex) {
            logger.log(Level.SEVERE, null, ex);
            result = -ErrorCodes.EIO();

        }
        return result;
    }

    @Override
    public int chown(String path, long uid, long gid) {
        System.out.println("Called chown in BFTFuse");
        int result;
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
             ObjectOutput objOut = new ObjectOutputStream(byteOut)) {
            objOut.writeObject(FSOperation.CHOWN);
            objOut.writeObject(path);
            objOut.writeObject(uid);
            objOut.writeObject(gid);
            objOut.flush();
            byteOut.flush();
            byte[] reply = serviceProxy.invokeOrdered(byteOut.toByteArray());
            if (reply.length == 0) {
                return -ErrorCodes.EIO();
            }
            try (ByteArrayInputStream byteIn = new ByteArrayInputStream(reply);
                 ObjectInput objIn = new ObjectInputStream(byteIn)) {
                result = (int)objIn.readObject();
            }
        } catch (IOException | ClassNotFoundException ex) {
            logger.log(Level.SEVERE, null, ex);
            result = -ErrorCodes.EIO();

        }
        return result;
    }



    @Override
    public int utimens(String path, Timespec[] timespec) {
        System.out.println("Called utimens in BFTFuse");
        int result;
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
             ObjectOutput objOut = new ObjectOutputStream(byteOut)) {
            long[] atimeval = {timespec[0].tv_sec.get(), timespec[0].tv_nsec.longValue() / 1000};
            long[] mtimeval = {timespec[1].tv_sec.get(), timespec[1].tv_nsec.longValue() / 1000};
            objOut.writeObject(FSOperation.UTIMES);
            objOut.writeObject(path);
            objOut.writeObject(atimeval);
            objOut.writeObject(mtimeval);
            objOut.flush();
            byteOut.flush();
            byte[] reply = serviceProxy.invokeOrdered(byteOut.toByteArray());
            if (reply.length == 0) {
                return -ErrorCodes.EIO();
            }
            try (ByteArrayInputStream byteIn = new ByteArrayInputStream(reply);
                 ObjectInput objIn = new ObjectInputStream(byteIn)) {
                result = (int)objIn.readObject();
            }
        } catch (IOException | ClassNotFoundException ex) {
            logger.log(Level.SEVERE, null, ex);
            result = -ErrorCodes.EIO();

        }
        return result;
    }

    @Override
    public int read(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {

        System.out.println("Called read in BFTFuse");
        int result;
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
             ObjectOutput objOut = new ObjectOutputStream(byteOut)) {
            objOut.writeObject(FSOperation.READ);
            objOut.writeObject(path);
            objOut.writeObject(size);
            objOut.writeObject(offset);
            objOut.writeObject(fi.flags.intValue());
            objOut.flush();
            byteOut.flush();
            System.out.println("Read made it 1");
            byte[] reply = serviceProxy.invokeOrdered(byteOut.toByteArray());
            if (reply.length == 0) {
                System.out.println("Read made it 2");
                return -ErrorCodes.EIO();
            }
            System.out.println("Read made it 3");
            try (ByteArrayInputStream byteIn = new ByteArrayInputStream(reply);
                 ObjectInput objIn = new ObjectInputStream(byteIn)) {
                System.out.println("Read made it 4");
                result = (int)objIn.readObject();
                System.out.println("Read made it 5");
                byte[] buffer = (byte[])objIn.readObject();
                System.out.println("Read made it 6");
                System.out.printf("The buffer in BFTFuse in read is %s\n", new String(buffer));
                buf.put(0, buffer, 0, result);
                System.out.printf("The buffer in BFTFuse in read (but after put call) is %s\n", new String(buffer));
            }
        } catch (IOException | ClassNotFoundException ex) {
            logger.log(Level.SEVERE, null, ex);
            result = -ErrorCodes.EIO();

        }
        return result;
    }

    @Override
    public int write(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
        System.out.println("Called write in BFTFuse");
        int result;
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
             ObjectOutput objOut = new ObjectOutputStream(byteOut)) {
            byte[] buffer = new byte[(int)size];
            buf.get(0, buffer, 0, (int)size);
            System.out.printf("The buffer in BFTFuse in write is %s\n", new String(buffer));
            objOut.writeObject(FSOperation.WRITE);
            objOut.writeObject(path);
            objOut.writeObject(buffer);
            objOut.writeObject(size);
            objOut.writeObject(offset);
            objOut.writeObject(fi.flags.intValue());
            objOut.flush();
            byteOut.flush();
            byte[] reply = serviceProxy.invokeOrdered(byteOut.toByteArray());
            if (reply.length == 0) {
                return -ErrorCodes.EIO();
            }
            try (ByteArrayInputStream byteIn = new ByteArrayInputStream(reply);
                 ObjectInput objIn = new ObjectInputStream(byteIn)) {
                result = (int)objIn.readObject();
            }
        } catch (IOException | ClassNotFoundException ex) {
            logger.log(Level.SEVERE, null, ex);
            result = -ErrorCodes.EIO();

        }
        return result;
    }


    @Override
    public int rmdir(String path) {
        System.out.println("Called rmdir in BFTFuse");
        int result;
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
             ObjectOutput objOut = new ObjectOutputStream(byteOut)) {
            objOut.writeObject(FSOperation.RMDIR);
            objOut.writeObject(path);
            objOut.flush();
            byteOut.flush();
            byte[] reply = serviceProxy.invokeOrdered(byteOut.toByteArray());
            System.out.printf("Sent path %s to rmdir and the reply is length: %d\n", path, reply.length);
            if (reply.length == 0) {
                return -ErrorCodes.EIO();
            }
            try (ByteArrayInputStream byteIn = new ByteArrayInputStream(reply);
                 ObjectInput objIn = new ObjectInputStream(byteIn)) {
                result = (int)objIn.readObject();
            }
        } catch (IOException | ClassNotFoundException ex) {
            logger.log(Level.SEVERE, null, ex);
            result = -ErrorCodes.EIO();

        }
        return result;
    }

    @Override
    public int readdir(String path, Pointer buf, FuseFillDir filter, @off_t long offset, FuseFileInfo fi) {
        System.out.println("Called readdir in BFTFuse");
        int result;
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
             ObjectOutput objOut = new ObjectOutputStream(byteOut)) {
            objOut.writeObject(FSOperation.READDIR);
            objOut.writeObject(path);
            objOut.flush();
            byteOut.flush();
            byte[] reply = serviceProxy.invokeOrdered(byteOut.toByteArray());
            System.out.printf("The reply length from readdir is %d\n", reply.length);
            if (reply.length == 0) {
                return -ErrorCodes.EIO();
            }
            try (ByteArrayInputStream byteIn = new ByteArrayInputStream(reply);
                 ObjectInput objIn = new ObjectInputStream(byteIn)) {
                result = (int)objIn.readObject();
                String[] filenames = (String[])objIn.readObject();
                for (String filename: filenames) {
                    filter.apply(buf, filename, null, 0);
                }
            }
        } catch (IOException | ClassNotFoundException ex) {
            logger.log(Level.SEVERE, null, ex);
            result = -ErrorCodes.EIO();

        }
        return result;
    }
/*
    public void translateFileStat(jnr.posix.FileStat jnrStat, FileStat fuseStat) {
        fuseStat.st_atim.tv_sec.set(jnrStat.atime());
        //fuseStat.st_birthtime.set
        fuseStat.st_blksize.set(jnrStat.blockSize());
        fuseStat.st_blocks.set(jnrStat.blocks());
        fuseStat.st_ctim.tv_sec.set(jnrStat.ctime());
        fuseStat.st_dev.set(jnrStat.dev());
        fuseStat.st_gid.set(jnrStat.gid());
        fuseStat.st_ino.set(jnrStat.ino());
        fuseStat.st_mode.set(jnrStat.mode());
        fuseStat.st_mtim.tv_sec.set(jnrStat.mtime());
        fuseStat.st_nlink.set(jnrStat.nlink());
        fuseStat.st_rdev.set(jnrStat.rdev());
        fuseStat.st_size.set(jnrStat.st_size());
        fuseStat.st_uid.set(jnrStat.uid());
    }
*/
    public static void main(String[] args) {
        int id = 1;
        BFTFuse fs = new BFTFuse(id);
        String path = "/home/cody/Desktop/mntp";
        fs.mount(Paths.get(path), true, true);
    }
}
