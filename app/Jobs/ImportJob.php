<?php

namespace App\Jobs;

use App\Events\ErrorFileDetected;
use App\Traits\ImportHelper;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Queue\Queueable;
use Illuminate\Support\Facades\File;

class ImportJob implements ShouldQueue
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
        if (File::exists($this->file . '.log'))
        {
            broadcast(new ErrorFileDetected($this->file));
        } else {
            $this->import08Concurrent($this->file);
        }

    }
}
