<?php

/**
 * Defines the class CodeRage\Db\Module
 *
 * File:        CodeRage/Db/Module.php
 * Date:        Wed Dec 16 19:52:11 UTC 2020
 * Notice:      This document contains confidential information
 *              and trade secrets
 *
 * @copyright   2020 CounselNow, LLC
 * @author      Jonathan Turkanis
 * @license     All rights reserved
 */

namespace CodeRage\Db;

use DOMDocument;
use DOMElement;
use CodeRage\Sys\ProjectConfig;
use CodeRage\Sys\Engine;
use CodeRage\Config;
use CodeRage\Db;
use CodeRage\Db\Params;
use CodeRage\Db\Schema\Generator;
use CodeRage\Db\Schema\Parser;
use CodeRage\Error;
use CodeRage\File;
use CodeRage\Util\Os;
use CodeRage\Xml;

/**
 * Database module
 */
final class Module extends \CodeRage\Sys\BasicModule {
    use \CodeRage\Xml\ElementCreator;

    /**
     * @var string
     */
    private const SCHEMA_PATH  = '.coderage/db/schema.dsx';

    /**
     * @var string
     */
    private const TEMPLATE_PATH  = __DIR__ . '/template.dsx';

    /**
     * @var string
     */
    private const METASCHEMA_PATH  = __DIR__ . '/Schema/dataSource.xsd';

    /**
     * Constructs an instance of CodeRage\Db\Module
     *
     * @param array $options The options array
     */
    public function __construct(array $options)
    {
        parent::__construct([
            'title' => 'Database',
            'description' => 'Database module'
        ]);
    }

    public function build(Engine $engine): void
    {
        $doc = $this->generateDatasourceDefinition($engine);
        $doc->formatOutput = true;
        $path = Config::projectRoot() . '/' . self::SCHEMA_PATH;
        File::mkdir(dirname($path));
        file_put_contents($path, $doc->saveXml());
        $engine->recordGeneratedFile($path);
    }

    public function install(Engine $engine): void
    {
        // Check if database exists
        $config = $engine->projectConfig();
        $database = $this->getProperty($config, 'db.database');
        if (in_array($database, Operations::listDatabases()))
            return;

        // Create database
        $options = ['xmltodb', '--create', '--non-interactive'];
        $options[] = '--username';
        $options[] = '%DATABASE_ADMIN_USERNAME';
        $options[] = '--password';
        $options[] = '%DATABASE_ADMIN_PASSWORD';
        foreach ($config->propertyNames() as $name) {
            $value = $this->getProperty($config, $name);
            $options[] = '--config';
            $options[] = escapeshellarg("$name=$value");
        }
        $options[] = Config::projectRoot() . '/' . self::SCHEMA_PATH;
        $command = join(' ', $options);
        $engine->log()->logMessage("running $command");
        $status = null;
        Os::run($command);

        // Grant permission
        $host = $this->getProperty($config, 'db.host');
        $database = $this->getProperty($config, 'db.database');
        $username = $this->getProperty($config, 'db.username');
        $password = $this->getProperty($config, 'db.password');
        $db =
           new Db([
                   'dbms' => 'mysql',
                   'host' => $host,
                   'database' => $database,
                   'username' => getenv('DATABASE_ADMIN_USERNAME'),
                   'password' => getenv('DATABASE_ADMIN_PASSWORD')
               ]);
        $sql =
            "GRANT ALL PRIVILEGES ON $database.*
             TO %s@%s IDENTIFIED BY '$password';";
        $db->query($sql, $username, '%');
    }

    /**
     * Returns a list of PHP-DI service definitions
     *
     * @param CodeRage\Sys\Engine $engine
     * @return array
     */
    public function services(Engine $engine): array
    {
        return ['db' => \DI\create(Db::class) ];
    }

    protected function namespaceUril(): ?string
    {
        return \CodeRage\Sys\NAMESPACE_URI;
    }

    /**
     * Generates and stores datasource definition
     *
     * @return DOMDocument
     */
    private function generateDatasourceDefinition(Engine $engine): DOMDocument
    {
        $doc =
            Xml::loadDocument(self::TEMPLATE_PATH, null, [
                'preserveWhitespace' => false
            ]);
        $doc->formatOutput = true;
        $root = $doc->documentElement;
        $root->insertBefore(
            $this->generateParamsDefinition($engine, $doc),
            Xml::firstChildElement($root)
        );
        $db = Xml::firstChildElement($root, 'database');
        foreach ($engine->moduleStore()->modules() as $module) {
            foreach ($module->tables() as $def) {
                $inner =
                    Xml::loadDocument($def, self::METASCHEMA_PATH, [
                        'preserveWhitespace' => false
                    ]);
                $common =
                    Xml::firstChildElement(
                        $inner->documentElement,
                        'commonColumns'
                    );
                $tables = $doc->importNode($inner->documentElement, true);
                foreach (Xml::childElements($tables, 'table') as $table) {
                    if ($common !== null) {
                        $first = Xml::firstChildElement($table, 'column');
                        foreach (Xml::childElements($common, 'column') as $col) {
                            $col = $doc->importNode($col, true);
                            $table->insertBefore($col, $first);
                        }
                    }
                    $db->appendChild($table);
                }
            }
        }
        return $doc;
    }

    /**
     * Generates the XML definition of the database connection parameters
     *
     * @return DOMElement
     */
    private function generateParamsDefinition(
        Engine $engine,
        DOMDocument $doc
    ): DOMElement {
        $params = Params::create($engine->projectConfig());
        $elt = $this->createElement($doc, 'connectionParams');
        $names = ['dbms', 'host', 'port', 'username', 'password', 'options'];
        foreach ($names as $name) {
            if (($value = $params->$name()) !== null) {
                if ($name !== 'options') {
                    $this->appendElement($elt, $name, "{config.db.$name}");
                } elseif (!empty($value)) {
                    foreach ($value as $n => $v) {
                        $option = $this->appendElement($elt, 'option', $v);
                        $option->setAttribute('name', $n);
                    }
                }
            }
        }
        return $elt;
    }

    /**
     * Returns the value of the named configuration variable
     *
     * @param CodeRage\Sys\ProjectConfig $config
     * @param string $name
     * @return string
     * @throws CodeRage\Sys\Error
     */
    private function getProperty(ProjectConfig $config, string $name)
    {
        $prop = $config->lookupProperty($name);
        if ($prop === null)
            throw new
                Error([
                    'status' => 'CONFIGURATION_ERROR',
                    'message' => "No such property: $name"
                ]);
        return $prop->value();
    }
}
