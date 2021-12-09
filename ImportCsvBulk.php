<?php

namespace ClickOffices\Jobs;

use DB;
use Log;
use Storage;
use Exception;
use ZipArchive;
use Carbon\Carbon;
use GuzzleHttp\Client;
use League\Csv\Reader;
use ClickOffices\Models\Office;
use Illuminate\Http\UploadedFile;
use ClickOffices\Models\WorkSpace;
use ClickOffices\Events\ImportOffices;
use Illuminate\Queue\SerializesModels;
use ClickOffices\Models\WorkSpaceMedia;
use ClickOffices\Models\UploadCsvImport;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Contracts\Queue\ShouldQueue;
use ClickOffices\Contracts\FileManager\FileManagerContract;

class ImportCsvBulk extends Job implements ShouldQueue
{
    use InteractsWithQueue, SerializesModels;

    /**
    * @var
    */
    protected $importer;

    /**
    * @var
    */
    protected $fileManager;

    /**
    * @var
    */
    protected $columns;

    /**
     * Create a new job instance.
     */
    public function __construct(UploadCsvImport $csvImport, FileManagerContract $fileManager)
    {
        $this->importer = $csvImport;
        $this->fileManager = $fileManager;
        $this->columns = collect([
            'Des' => 0,
            'Availability' => 1,
            'Type' => 2,
            'Off Num' => 3,
            'From' => 4,
            'To' => 5,
            'Price' => 6,
            'Currency' => 7,
            'Size (sq ft)' => 8,
            'Ref' => 9,
        ]);
    }

    /**
     * Execute the job.
     */
    public function handle()
    {
        $url = $this->importer->getPublicUrl();
        $path = $this->unZipFile($url);

        $csvList = $this->getAllCsv($path);
        $images = $this->getAllImages($path);
        $notifications = [];
        $processedOffices = [];
        try {
            DB::beginTransaction();

            foreach ($csvList as $csv) {
                $csv = Reader::createFromPath($csv);
                $csv = $csv->setDelimiter(';');
                $failed = 0;
                foreach ($csv->fetch() as $index => $row) {
                    // Ignore header
                    if ($index == 0) {
                        continue;
                    }

                    $reference = trim(array_get($row, $this->columns->get('Ref')));
                    $sanitizedData = $this->sanitizeRow($row);

                    // Ignore when Office (centre) does not exit
                    $centre = Office::find($reference);
                    if (!$centre) {
                        $notifications[] = 'There is no centre with ID #'.$reference.'.';

                        continue;
                    }

                    $office = WorkSpace::where('office_id', $reference)
                        ->where('office_number', array_get($sanitizedData, $this->columns->get('Off Num')))
                        ->first();

                    if (null === $office) {
                        $office = new WorkSpace();
                        $office->office_id = $reference;
                        $office->office_number = array_get($sanitizedData, $this->columns->get('Off Num'));
                    }

                    $office->type = array_get($sanitizedData, $this->columns->get('Type'));
                    $office->availability = array_get($sanitizedData, $this->columns->get('Availability'));
                    $office->description = array_get($sanitizedData, $this->columns->get('Des'));
                    $office->desk_from = array_get($sanitizedData, $this->columns->get('From'));
                    $office->desk_to = array_get($sanitizedData, $this->columns->get('To'));
                    $office->price = array_get($sanitizedData, $this->columns->get('Price'));
                    $office->currency = array_get($sanitizedData, $this->columns->get('Currency'));
                    $office->size = array_get($sanitizedData, $this->columns->get('Size (sq ft)'));
                    $office->status = true;
                    $office->disp_on_web = true;

                    if (!$office->save()) {
                        $notifications[] = 'Office '.array_get($sanitizedData, $this->columns->get('Off Num')).' for center '.$reference.' was not saved.';
                        ++$failed;
                    }

                    if (!isset($processedOffices[$reference])) {
                        $processedOffices[$reference] = [];
                    }
                    $processedOffices[$reference][] = $office->id;
                }
            }
            if ($failed > 0) {
                throw new Exception('Some offices could not be processed.');
            }

            // Mark as disabled all offices not present in the file for the given references
            foreach ($processedOffices as $centreId => $processedOfficeIds) {
                $centreOffices = WorkSpace::where('office_id', $centreId)->get();
                foreach ($centreOffices as $centreOffice) {
                    if (in_array($centreOffice->id, $processedOfficeIds)) {
                        continue;
                    }
                    if ($centreOffice->rem_unchanged_csv == 1) {
                        continue;
                    }

                    $centreOffice->status = false;
                    $centreOffice->disp_on_web = false;
                    $centreOffice->save();
                }
            }
            foreach ($images as $image) {
                list($dirname, $basename, $extension, $filename) = array_flatten(pathinfo($image), INF);

                $mime = mime_content_type($image) ?: 'image/jpeg';
                if (!starts_with($extension, '.')) {
                    $extension = '.'.$extension;
                }

                $processedWorkspacesIds = array_flatten($processedOffices);
                $processedWorkspaces = WorkSpace::whereIn('id', $processedWorkspacesIds)->get();

                foreach ($processedWorkspaces as $workspace) {
                    $fileNameSlug = str_slug($filename);
                    $officeNumberSlug = str_slug($workspace->office_number);
                    //search if image filename contains the office number
                    $contains = str_contains($fileNameSlug, $officeNumberSlug);

                    if (!$contains) {
                        continue;
                    }

                    // Check if already exist an image assigned to this workspace
                    $media = WorkSpaceMedia::where('workspace_id', $workspace->id)->first();

                    if (null === $media) {
                        //Upload image to S3
                        $uploadedFile = new UploadedFile($image, $filename.$extension, $mime, filesize($image), null, true);

                        $url = $this->fileManager->uploadFile(
                            $uploadedFile,
                            config('media.s3.workspaces-media-zip-path')
                        );

                        //if filename contains the office number of the workspace then assign id
                        $media = new WorkSpaceMedia();
                        $media->workspace_id = $workspace->id;
                        $media->name = $filename;
                        $media->type = $mime;
                        $media->url = $url;

                        if (!$media->save()) {
                            $notifications[] = 'Media '.$filename.' for office '.$workspace->id.' was not saved.';
                        }
                    }
                }
            }

            DB::commit();
        } catch (\Exception $e) {
            \Log::info($e->getLine() . ' / ' . $e->getMessage());
            $notifications[] = 'Something went wrong: ' . $e->getLine() . ' / ' . $e->getMessage();
            DB::rollBack();
        }

        remove_directory($path);
        //Email notification
        event(new ImportOffices(['csv' => $this->importer, 'notifications' => array_unique($notifications)]));
    }

    /**
     * Get all CSV files
     *
     * @return array
     * @param  mixed $path
     */
    private function getAllCsv($path): array
    {
        $filepaths = rsearch($path, ['csv']);
        $fileNames = [];
        foreach ($filepaths as $filepath) {
            $fileNames[] = $filepath->getPathName();
        }

        return $fileNames;
    }

    /**
     * Get all Images.
     *
     * @return array
     * @param  mixed $path
     */
    private function getAllImages($path): array
    {
        $filepaths = rsearch($path, ['jpg', 'png', 'bmp']);
        $fileNames = [];
        foreach ($filepaths as $filepath) {
            $fileNames[] = $filepath->getPathName();
        }

        return $fileNames;
    }

    /**
     * Prepare the row
     *
     * @param array $row
     *
     * @return array
     */
    private function sanitizeRow(array $row): array
    {
        $result = [];
        foreach ($this->columns as $column) {
            switch ($column) {
                case $this->columns->get('Currency'):
                    $currency = utf8_encode(trim($row[$column]));
                    if (str_contains($currency, '£')) {
                        $result[$column] = 'GBP';
                    } elseif (str_contains($currency, '€')) {
                        $result[$column] = 'EUR';
                    } else {
                        $result[$column] = '';
                    }
                    break;
                case $this->columns->get('Availability'):
                    $availability = trim($row[$column]);
                    if (empty($availability)) {
                        $result[$column] = null;
                    } else {
                        try {
                            $result[$column] = Carbon::createFromFormat('d/m/Y', $availability)->format('Y-m-d');
                        } catch (Exception $e) {
                            $result[$column] = null;
                        }
                    }
                    break;
                case $this->columns->get('Type'):
                    $result[$column] = array_search(trim($row[$column]), config('office.work-spaces.type'));
                    break;
                case $this->columns->get('Price'):
                    $result[$column] = str_replace(',', '', trim($row[$column]));
                    break;
                case $this->columns->get('Des'):
                    $result[$column] = utf8_encode(trim($row[$column]));
                    break;
                case $this->columns->get('Size (sq ft)'):
                    $result[$column] = str_replace(',', '', trim($row[$column]));
                    break;
                default:
                    $result[$column] = trim($row[$column]);
            }
        }

        return $result;
    }

    /**
     * Unzip file from AWS
     *
     * @param string $fileUrl
     *
     * @return string
     */
    private function unZipFile(string $fileUrl): string
    {
        $guzzle = new Client();
        $response = $guzzle->get($fileUrl);
        $tempFolderName = time();
        $tempFileName = $tempFolderName.'_csvfile.zip';
        Storage::disk('local')->put($tempFileName, $response->getBody());

        $zip = new ZipArchive;
        $res = $zip->open(storage_path("app/{$tempFileName}"));

        if ($res === true) {
            $path = storage_path('app/unzip/'.$tempFolderName);
            $zip->extractTo($path);
            $zip->close();
        } else {
            Log::error('Oops, something went wrong in the process of unzip this file');
        }
        Storage::disk('local')->delete($tempFileName);

        return $path;
    }
}
