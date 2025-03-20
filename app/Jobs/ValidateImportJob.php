<?php

namespace App\Jobs;

use App\Models\Person;
use App\Traits\ImportHelper;
use Carbon\Carbon;
use Illuminate\Bus\Batchable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Queue\Queueable;
use Illuminate\Support\Facades\Log;

class ValidateImportJob implements ShouldQueue
{
    use Queueable, ImportHelper;

    /**
     * Create a new job instance.
     */
    public function __construct(public string $file)
    {
        //
    }

    /**
     * Execute the job.
     */
    public function handle(): void
    {
        $this->validateImport($this->file);
    }
}
