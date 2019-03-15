import bftsmart.tom.ServiceProxy;
import jnr.ffi.Pointer;
import jnr.ffi.types.mode_t;
import jnr.ffi.types.off_t;
import jnr.ffi.types.size_t;
import jnr.posix.POSIX;
import jnr.posix.POSIXFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;
import ru.serce.jnrfuse.struct.Timespec;
import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;

public class BFTFuse extends FuseStubFS {

    private Logger logger;
    POSIX posix;
    ServiceProxy serviceProxy;

    public static void main(String[] args) {
        if(args.length < 2) {
            System.out.println("Use: java BFTFuse <clientID> <mountPath>");
            System.exit(-1);
        }
        new BFTFuse(Integer.parseInt(args[0]), args[1]);
    }

    public BFTFuse(int clientID, String mountPath) {
        logger = LoggerFactory.getLogger(this.getClass());
        Path  mountPoint = Paths.get(mountPath);
        try {
            mountPoint = mountPoint.toRealPath();
        } catch (IOException e) {
            logger.error("Unable to resolve provided path: ", e);
            System.exit(-1);;
        }
        posix = POSIXFactory.getPOSIX();
        serviceProxy = new ServiceProxy(clientID, "config");
        this.mount(mountPoint, true, true);
    }

    @Override
    public int getattr(String path, FileStat stat) {
        logger.debug("Called gettattr with path: " + path);
        int result;
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
             ObjectOutput objOut = new ObjectOutputStream(byteOut)) {

            objOut.writeObject(FSOperation.GETATTR);
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
                //If result != 0 the stat is random garbage so we didn't send it
                if (result == 0) {
                    //Omitting atime, ctime, blocks, blockSize, mtime, dev, rdev, ino, since they can break
                    //consensus. Some of these could potentially be added back depending on the configuration
                    stat.st_gid.set((int)objIn.readObject());
                    stat.st_mode.set((int) objIn.readObject());
                    stat.st_nlink.set((int) objIn.readObject());
                    stat.st_size.set((long)objIn.readObject());
                    stat.st_uid.set((int)objIn.readObject());
                }
            }
        }catch (IOException | ClassNotFoundException ex) {
            logger.error(null, ex);
            result = -ErrorCodes.EIO();
        }
        return result;
    }

    @Override
    public int mkdir(String path, @mode_t long mode) {
        logger.debug("Called mkdir with path: " + path);
        int result;
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
             ObjectOutput objOut = new ObjectOutputStream(byteOut)) {

            objOut.writeObject(FSOperation.MKDIR);
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
            logger.error(null, ex);
            result = -ErrorCodes.EIO();
        }
        return result;
    }

    @Override
    public int rename(String oldpath, String newpath) {
        logger.debug("Called rename with old path: " + oldpath + " new path: " + newpath);
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
            logger.error(null, ex);
            result = -ErrorCodes.EIO();
        }
        return result;
    }

    @Override
    public int unlink(String path) {
        logger.debug("Called unlink with path: " + path);
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
            logger.error(null, ex);
            result = -ErrorCodes.EIO();
        }
        return result;
    }

    @Override
    public int truncate(String path, long size) {
        logger.debug("Called truncate with path: " + path);
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
            logger.error(null, ex);
            result = -ErrorCodes.EIO();
        }
        return result;
    }

    @Override
    public int open(String path, FuseFileInfo fi) {
        logger.debug("Called open with path: " + path);
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
            logger.error(null, ex);
            result = -ErrorCodes.EIO();
        }
        return result;
    }

    @Override
    public int create(String path, @mode_t long mode, FuseFileInfo fi) {
        logger.debug("Called create with path: " + path);
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
            logger.error(null, ex);
            result = -ErrorCodes.EIO();
        }
        return result;
    }

    @Override
    public int chmod(String path, long mode) {
        logger.debug("Called chmod with path: " + path);
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
            logger.error(null, ex);
            result = -ErrorCodes.EIO();
        }
        return result;
    }

    @Override
    public int chown(String path, long uid, long gid) {
        logger.debug("Called chown with path: " + path);
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
            logger.error(null, ex);
            result = -ErrorCodes.EIO();
        }
        return result;
    }

    @Override
    public int utimens(String path, Timespec[] timespec) {
        logger.debug("Called utimens with path: " + path);
        int result;
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
             ObjectOutput objOut = new ObjectOutputStream(byteOut)) {
            //access and modification times are being updated but aren't being returned by getattr because of
            //the potential to break consensus when outside factors update them.
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
            logger.error(null, ex);
            result = -ErrorCodes.EIO();
        }
        return result;
    }

    @Override
    public int read(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
        logger.debug("Called read with path: " + path);
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
            byte[] reply = serviceProxy.invokeOrdered(byteOut.toByteArray());
            if (reply.length == 0) {
                return -ErrorCodes.EIO();
            }
            try (ByteArrayInputStream byteIn = new ByteArrayInputStream(reply);
                 ObjectInput objIn = new ObjectInputStream(byteIn)) {
                result = (int)objIn.readObject();
                byte[] buffer = (byte[])objIn.readObject();
                buf.put(0, buffer, 0, result);
            }
        } catch (IOException | ClassNotFoundException ex) {
            logger.error(null, ex);
            result = -ErrorCodes.EIO();
        }
        return result;
    }

    @Override
    public int write(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
        logger.debug("Called write with path: " + path);
        int result;
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
             ObjectOutput objOut = new ObjectOutputStream(byteOut)) {
            byte[] buffer = new byte[(int)size];
            buf.get(0, buffer, 0, (int)size);

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
            logger.error(null, ex);
            result = -ErrorCodes.EIO();
        }
        return result;
    }

    @Override
    public int rmdir(String path) {
        logger.debug("Called rmdir with path: " + path);
        int result;
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
             ObjectOutput objOut = new ObjectOutputStream(byteOut)) {

            objOut.writeObject(FSOperation.RMDIR);
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
            logger.error(null, ex);
            result = -ErrorCodes.EIO();
        }
        return result;
    }

    @Override
    public int readdir(String path, Pointer buf, FuseFillDir filter, @off_t long offset, FuseFileInfo fi) {
        logger.debug("Called readdir with path: " + path);
        int result;
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
             ObjectOutput objOut = new ObjectOutputStream(byteOut)) {
            objOut.writeObject(FSOperation.READDIR);
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
                String[] filenames = (String[])objIn.readObject();
                for (String filename: filenames) {
                    filter.apply(buf, filename, null, 0);
                }
            }
        } catch (IOException | ClassNotFoundException ex) {
            logger.error(null, ex);
            result = -ErrorCodes.EIO();
        }
        return result;
    }
}
