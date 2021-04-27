<?php

/**
 * Defines the class CodeRage\File
 *
 * File:        CodeRage/File.php
 * Date:        Thu Sep 17 12:28:44 UTC 2020
 * Notice:      This document contains confidential information and
 *              trade secrets
 * @copyright   2020 CounselNow, LLC
 * @author      Jonathan Turkanis
 * @license     All rights reserved
 */

namespace CodeRage;

use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;
use CodeRage\Config;
use CodeRage\Error;
use CodeRage\Util\Args;
use CodeRage\Util\ErrorHandler;
use CodeRage\Util\Os;
use CodeRage\Util\Random;

/**
 * Container for static methods for filesystem access
 */
final class File {

    /**
     * Canonicalizes the given file pathname
     *
     * @param string $path
     * @throws CodeRage\Error if $path does not exist
     * @todo Eliminate after replacing build system
     */
    public static function canonicalize(string $path) : string
    {
        if (!file_exists($path))
            throw new Error(['details' => "No such file: $path"]);
        return realpath($path);
    }

    /**
     * Throws an exception if the specified file does not exist, is not a
     * directory, or is not sufficiently accessible by the current user
     *
     * @param string $path The directory path
     * @param string $mode A bitwise OR of zero or more of the values
     *     1 - executable
     *     2 - writable
     *     4 - readable
     *   Each bit that is present imposes a separate accessibility requirement
     *   on $path.
     * @throws CodeRage\Error
     */
    public static function checkDirectory(string $path, int $mode = 0) : void
    {
        Args::check($path, 'string', 'directory path');
        if (!file_exists($path))
            throw new
                Error([
                    'status' => 'OBJECT_DOES_NOT_EXIST',
                    'message' => "No such directory: $path"
                ]);
        if (!is_dir($path))
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'message' => "The file '$path' is not a directory"
                ]);
        if ($mode !== null) {
            Args::check($mode, 'int', 'file mode');
            if (($mode & 1) !== 0 && !is_executable($path))
                throw new
                    Error([
                        'status' => 'INVALID_PARAMETER',
                        'message' => "The directory '$path' is not executable"
                    ]);
            if (($mode & 2) !== 0 && !is_writable($path))
                throw new
                    Error([
                        'status' => 'INVALID_PARAMETER',
                        'message' => "The directory '$path' is not writable"
                    ]);
            if (($mode & 4) !== 0 && !is_readable($path))
                throw new
                    Error([
                        'status' => 'INVALID_PARAMETER',
                        'message' => "The directory '$path' is not readable"
                    ]);
        }
    }

    /**
     * Throws an exception if the specified file does not exist, is not a plain
     * file, or is not accessible by the current user
     *
     * @param string $path The file path
     * @param string mode A bitwise OR of zero or more of the values
     *     1 - executable
     *     2 - writable
     *     4 - readable
     *   Each bit that is present imposes a separate accessibility requirement
     *   on $path.
     * @throws CodeRage\Error
     */
    public static function checkFile(string $path, int $mode = null) : void
    {
        Args::check($path, 'string', 'file path');
        if (!file_exists($path))
            throw new
                Error([
                    'status' => 'OBJECT_DOES_NOT_EXIST',
                    'message' => "No such file: $path"
                ]);
        if (!is_file($path))
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'message' =>
                        "The file '$path' is " .
                        (is_dir($path) ? 'a directory' : 'not a plain file')
                ]);
        if ($mode !== null) {
            Args::check($mode, 'int', 'file mode');
            if (($mode & 1) !== 0 && !is_executable($path))
                throw new
                    Error([
                        'status' => 'INVALID_PARAMETER',
                        'message' => "The file '$path' is not executable"
                    ]);
            if (($mode & 2) !== 0 && !is_writable($path))
                throw new
                    Error([
                        'status' => 'INVALID_PARAMETER',
                        'message' => "The file '$path' is not writable"
                    ]);
            if (($mode & 4) !== 0 && !is_readable($path))
                throw new
                    Error([
                        'status' => 'INVALID_PARAMETER',
                        'message' => "The file '$path' is not readable"
                    ]);
        }
    }


    /**
     * Throws an exception if the specified file does not exist, is a directory,
     * or is not readable
     *
     * @param string $path The file path
     * @throws CodeRage\Error
     */
    public static function checkReadable(string $path) : void
    {
        self::checkFile($path, 0b100);
    }

    /**
     * Copies the given file or directory to the given destination.
     *
     * @param string $src
     * @param string $dest
     * @throws Exception
     */
    public static function copy(string $src, string $dest) : void
    {
        $handler = new ErrorHandler;

        // Case 1: $src is a file
        self::checkDirectory(dirname($dest), 0b0111);
        if (is_file($src)) {
            if (is_dir($dest)) {
                throw new
                    Error([
                        'status' => 'INVALID_PARANETER',
                        'details' =>
                            "Failed copying '$src' to '$dest': the file " .
                            "'$dest' is a directory"
                    ]);
            }
            $result = $handler->_copy($src, $dest);
            if (!$result || $handler->errno()) {
                $msg = "Failed copying '$src' to '$dest'";
                throw new
                    Error([
                        'status' => 'FILESYSTEM_ERROR',
                        'details' => $handler->formatError($msg)
                    ]);
            }
            self::chmod($dest, self::perms($src), $handler);
            return;
        }

        // Case 2: $src is a directory (see https://bit.ly/3cTG6np)
        self::mkdir($dest, self::perms($src), $handler);
        $flags = RecursiveDirectoryIterator::SKIP_DOTS;
        $it =
            new RecursiveIteratorIterator(
                    new RecursiveDirectoryIterator($src, $flags),
                    RecursiveIteratorIterator::SELF_FIRST
                );
        foreach ($it as $info) {
            $s = $info->getPathname();
            $d = $dest . DIRECTORY_SEPARATOR . $it->getSubPathname();
            $m = self::perms($s);
            if ($info->isDir()) {
                self::mkdir($d, $m, $handler);
            } else {
                $result = $handler->_copy($s, $d);
                if (!$result || $handler->errno()) {
                    $msg = "Failed copying '$s' to '$d'";
                    throw new
                        Error([
                            'status' => 'FILESYSTEM_ERROR',
                            'details' => $handler->formatError($msg)
                        ]);
                }
                self::chmod($d, $m, $handler);
            }
        }
    }

    /**
     * Returns the list of executables with the given root name in the given
     * listof directories.
     *
     * @param string $program The program name, minus the windows file extension
     * @param array $search The directory or list of directories to search;
     *   defaults to the directories in the current user's path.
     * @return array
     * @todo Eliminate after replacing build system
     */
    public static function findExecutable(string $program, array $search = null)
        : array
    {
        $posix = Os::type() == 'posix';
        if (!$search) {
            $search = explode(PATH_SEPARATOR, getenv('PATH'));
        } elseif (is_string($search)) {
            $search = [$search];
        }
        $extensions = $posix ?
            [''] :
            explode(';', strtolower(getenv('PATHEXT')));
        $sep = $posix ? '/' : '\\';
        $result = [];
        foreach ($search as $dir) {
            foreach ($extensions as $ext) {
                $file = "$dir$sep$program$ext";
                if (is_file($file) && (!$posix || is_executable($file))) {
                    $result[] = realpath($file);
                    break;
                }
            }
        }
        return array_unique($result);
    }


    /**
     * Writes the given file content to the specified file.
     *
     * @param string $path The file.
     * @param string $content
     * @param string $type One of the strings 'bat', 'c', 'cs', 'css', 'c++',
     *   'htm', 'html', 'ini', 'java', 'js', 'php', 'pl', 'pm', 'py', or 'xml'.
     * @throws Exception
     * @todo Eliminate after replacing build system
     */
    public static function generate(string $path, string $content,
        ?string $type = null) : void
    {
        // Try to guess type
        $guess = $type === null;
        if ($guess) {
            $match = null;
            if (!preg_match('#[^/\\.]\.([^/\\.]+)$#', $path, $match))
                throw new
                    Error([
                        'status' => 'INCONSISTENT_PARAMETERS',
                        'details' => "Can't guess type of file '$path'"
                    ]);
            $type = $match[1];
        }

        // Define prefix
        $prefix = null;
        $warning = 'AUTOMATICALLY GENERATED BY CODERAGE TOOLS - DO NOT EDIT';
        $timestamp = date(DATE_W3C);
        $copyright = 'Copyright CodeRage';
        $n = Os::type() == 'posix' ? "\n" : "\r\n";
        switch (strtolower($type)) {
        case 'bat':
            $prefix = ":: $warning$n:: $timestamp$n::$copyright$n";
            break;
        case 'c':
        case 'cs':
        case 'css':
        case 'c++':
        case 'java':
        case 'js':
            $prefix = "/* $warning */$n/* $timestamp */$n/* $copyright */$n";
            break;
        case 'htm':
        case 'html':
            $prefix =
                "<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Strict//EN\"
                    \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd\">$n" .
                "<!-- $warning -->$n<!-- $timestamp -->$n<!-- $copyright -->$n";
            break;
        case 'ini':
            $prefix = "; $warning$n; $timestamp$n; $copyright$n";
            break;
        case 'php':
            $prefix =
                "<?php$n/* $warning */$n/* $timestamp */$n/* $copyright */$n";
            $content = preg_replace('/^<\?php[^\n]*\n/', '', $content);
            break;
        case 'pl':
        case 'pm':
        case 'py':
            $prefix = "# $warning$n# $timestamp$n# $copyright$n";
            break;
        case 'xml':
            $prefix =
                   '<?xml version="1.0" encoding="UTF-8"?>' . "$n$n" .
                   "<!-- $warning -->$n<!-- $timestamp -->$n" .
                   "<!-- $copyright -->$n";
            break;
        default:
            $msg = $guess ?
               "Can't guess type of file '$path'" :
               "Unknown file type: $type";
            throw new
                Error([
                    'status' => 'INCONSISTENT_PARAMETERS',
                    'details' => $msg
                ]);
        }

        // Fix line endings
        $content = str_replace("\r", '', $content);
        if ($n == "\r\n")
            $content = str_replace("\n", "\r\n", $content);

        // Write file
        self::mkdir(dirname($path));
        if (!@file_put_contents($path, "$prefix$content"))
            throw new
                Error([
                    status => 'FILESYSTEM_ERROR',
                    details => "Failed writing to file '$path'"
                ]);
    }


    /**
     * Returns true if the given file pathname is absolute.
     *
     * @param string $path
     * @return boolean
     */
    public static function isAbsolute(string $path) : bool
    {
        return Os::type() == 'posix' ?
            $path && $path[0] == '/' :
            $path && $path[0] == '/' ||
                $path && $path[0] == '\\' ||
                strlen($path) > 1 && ctype_alpha($path[0]) && $path[1] == ':';
    }

    /**
     * Returns the result of concatenating the file paths specified either as
     * a sequence of function arguments or as an array.
     *
     * @param string ...$args A list of file paths
     * @return string
     */
    public static function join(string ...$path) : string
    {
        return join(DIRECTORY_SEPARATOR , $path);
    }

    /**
     * Calls built-in mkdir() with $recurisve == true
     *
     * @param string $path The directory to created
     * @param int $mode The file mode
     * @throws CodeRage\Error
     */
    public static function mkdir(string $path, int $mode = 0777, ?ErrorHandler $handler = null) : void
    {
        if ($handler === null) {
            $handler = new ErrorHandler;
        }
        $result = $handler->_mkdir($path, $mode, true);
        if (!is_dir($path)) {
            $msg = "Failed creating directory '$path'";
            throw new
                Error([
                    'status' => 'FILESYSTEM_ERROR',
                    'details' => $handler->formatError($msg)
                ]);
        }
    }

    /**
     * Resolves the given file path relative to the given base path
     *
     * @param string $path The path
     * @param string $base The base path
     * @param boolean $canonicalize true to check if the resulting file exists
     *   and canonicalize it
     * @return string
     */
    public static function resolve(string $path, string $base,
        ?bool $canonicalize = false) : string
    {
        if (strlen($path) == 0)
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'message' => 'Empty path'
                ]);
        $resolved = $sep = null;
        if (self::isAbsolute($path)) {
            throw new
                Error([
                    'status' => 'INVALID_PARAMETER',
                    'message' => 'Empty path'
                ]);
            $resolved = $path;
        } else {
            $sep = Os::type() == 'posix' ? '/' : '\\';
            $resolved = dirname($base) . $sep . $path;
        }
        if ($canonicalize) {
            if (!self::isAbsolute($resolved)) {
                if ($sep === null)
                    $sep = Os::type() == 'posix' ? '/' : '\\';
                $resolved = getcwd() . $sep . $resolved;
            }
            if (($resolved = realpath($resolved)) === false)
                throw new
                    Error([
                        'status' => 'OBJECT_DOES_NOT_EXIST',
                        'message' => "No such file: $path"
                    ]);
        }
        return $resolved;
    }

    /**
     * Removes the given file, and all the files it contains, if it is a
     * directory. Replacement for system("rm -fr $path")
     *
     * @param string $path
     * @return boolean true if the file does not exist when execution completes
     * @todo Eliminate after replacing build system
     */
    public static function rm(string $path) : bool
    {
        if (is_file($path) || is_link($path)) {
            return  @unlink($path) ? true : !file_exists($path);
        } elseif (is_dir($path)) {
            if ($dir = dir($path)) {
                while (false !== ($file = $dir->read()))
                    if ($file != '.' && $file != '..')
                        self::rm("$path/$file");
                $dir->close();
            }
            return @rmdir($path) ? true : !file_exists($path);
        } else {
            return true;
        }
    }

    /**
     * Returns the file permissions of the specified file
     *
     * @param string $path The file pathname
     * @param CodeRage\Util\ErrorHandler $handler An error handler
     * @return int
     * @throws CodeRgae\Error
     */
    public static function perms(string $path, ?ErrorHandler $handler = null): int
    {
        if ($handler === null) {
            $handler = new ErrorHandler;
        }
        $result = $handler->_fileperms($path);
        if (!$result || $handler->errno()) {
            $msg = "Failed retrieving permissions of '$path'";
            throw new
                Error([
                    'status' => 'FILESYSTEM_ERROR',
                    'details' => $handler->formatError($msg)
                ]);
        }
        return $result;
    }

    /**
     * Sets the permissions of file
     *
     * @param string $path The file pathname
     * @param int $mode The desired UNIX file permissions
     * @param CodeRage\Util\ErrorHandler $handler An error handler
     */
    public static function chmod(string $path, int $mode, ?ErrorHandler $handler = null) : void
    {
        if ($handler === null) {
            $handler = new ErrorHandler;
        }
        $result = $handler->_chmod($path, $mode);
        if (!$result || $handler->errno()) {
            $msg = sprintf('%s%o', "Failed setting permissions of '$path' to ", $mode);
            throw new
                Error([
                    'status' => 'FILESYSTEM_ERROR',
                    'details' => $handler->formatError($msg)
                ]);
        }
    }

    /**
     * Searches the PHP include_path for a file with the given relative path
     *
     * @param string $path The pathname
     * @param boolean $ignoreRelativePaths true if only absolute paths in the
     *   PHP include path should be searched.
     * @return string The absolute pathname, or null if no file was found
     * @todo Eliminate after replacing build system
     */
    public static function searchIncludePath(string $path,
        bool $ignoreRelativePaths = false) : ?string
    {
        $include = ini_get('include_path');
        $search = [];
        foreach (explode(PATH_SEPARATOR, ini_get('include_path')) as $p) {
            if (self::isAbsolute($p)) {
                $search[] = $p;
            } elseif (!$ignoreRelativePaths) {
                if ($p2 = self::resolve($p, getcwd()))
                    $search[] = $p2;
            }
        }
        foreach ($search as $dir)
            if (file_exists($p = File::resolve($path, $dir)))
                return $p;
        return $p;
    }

    /**
     * Returns the pathname of a newly created file in the system temporary
     * file directory that will be deleted when the current request terminates.
     * If no file extension is specified, the file will be created using
     * tempname().
     *
     * @param string $prefix The prefix to pass to tempnam.
     * @param string $ext The file extension, if any; any empty string can be used
     * ccto indicate that the returned file path should have no extension.
     * @return string
     * @throws CodeRage\Error
     */
    public static function temp(?string $prefix = '', ?string $ext = null,
        bool $cleanup = false) : ?string
    {
        static $paths;
        if ($paths === null) {
            $paths = [];
            register_shutdown_function(function() {
                self::temp(null, null, true);
            });
        }

        // Delete files on exit
        if ($cleanup) {
            foreach ($paths as $p)
                if (file_exists($p))
                    @unlink($p);
            return null;
        }

        // Construct path
        $path = null;
        if ($ext !== null) {
            $dir = sys_get_temp_dir();
            if ($ext)
                $ext = ".$ext";
            $rand = $prefix;
            for ($z = 0; $z < 10; ++$z) {
                $rand .= Random::string(10);
                if (!file_exists($file = "$dir/$rand$ext")) {
                    @touch($file);
                    @chmod($file, 0600);
                    if ( is_file($file) &&
                         ( Os::type() == 'windows' ||
                           (fileperms($file) & 0777) == 0600 ) )
                    {
                        $path = $file;
                        break;
                    }
                }
            }
            if (!$path)
                throw new
                    Error([
                        'details' => "Failed creating temporary file: $file"
                    ]);
        } else {
            if (($p = tempnam(sys_get_temp_dir(), $prefix)) !== false) {
                $paths[] = $p;
                return $p;
            } else {
                throw new
                    Error([
                        'details' => 'Failed creating temporary file'
                    ]);
            }
        }

        $paths[] = $path;
        return $path;
    }

    /**
     * Returns the pathname of a newly created directory in the system temporary
     * file directory that will be deleted when the current request terminates
     *
     * @param string $prefix The prefix, if any.
     * @throws CodeRage\Error
     */
    public static function tempDir(?string $prefix = '', bool $cleanup = false)
        : ?string
    {
        static $paths;
        if ($paths === null) {
            $paths = [];
            register_shutdown_function(function() {
                self::tempDir(null, true);
            });
        }

        // Delete files on exit
        if ($cleanup) {
            foreach ($paths as $p)
                self::rm($p);
            return null;
        }

        // Construct path
        $handler = new ErrorHandler;
        $dir = sys_get_temp_dir();
        $path = null;
        $rand = $prefix;
        for ($z = 0; $z < 10; ++$z) {
            $rand .= Random::string(10);
            if (!file_exists($file = "$dir/$rand")) {
                self::mkdir($file, 0700, $handler);
                if ( is_dir($file) &&
                     ( Os::type() == 'windows' ||
                       (self::perms($file) & 0777) == 0700 ) )
                {
                    $path = $file;
                    break;
                }
            }
        }
        if (!$path)
            throw new
                \CodeRage\Error([
                    'status' => 'FILESYSTEM_ERROR',
                    'message' => 'Failed creating temporary directory'
                ]);

        $paths[] = $path;
        return $path;
    }
}
